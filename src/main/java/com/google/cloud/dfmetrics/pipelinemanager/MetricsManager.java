/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dfmetrics.pipelinemanager;

import com.google.api.client.util.ArrayMap;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMessage;
import com.google.api.services.dataflow.model.ListJobMessagesResponse;
import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.auto.value.AutoValue;
import com.google.cloud.dfmetrics.model.ResourcePricing;
import com.google.cloud.dfmetrics.utils.RetryUtil;
import com.google.monitoring.v3.TimeInterval;
import com.google.protobuf.util.Timestamps;
import dev.failsafe.Failsafe;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class {@link MetricsManager} encapsulates all Dataflow Job metrics related operations. */
@AutoValue
public abstract class MetricsManager {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsManager.class);
  private static final Pattern CURRENT_METRICS = Pattern.compile(".*Current.*");

  private static final String DATAFLOW_SERVICEMETRICS_NAMESPACE = "dataflow/v1b3";

  private static final Duration MONITORING_METRICS_DELAY_DURATION = Duration.ofSeconds(5);

  private static final Pattern WORKER_START_PATTERN =
      Pattern.compile(
          "^All workers have finished the startup processes and began to receive work requests.*$");
  private static final Pattern WORKER_STOP_PATTERN = Pattern.compile("^Stopping worker pool.*$");

  private static final double scale = Math.pow(10, 3);

  public static MetricsManager.Builder builder(
      Dataflow dataflow, Job job, MonitoringClient monitoringClient) {
    return new AutoValue_MetricsManager.Builder()
        .setDataflowClient(dataflow)
        .setJob(job)
        .setMonitoringMetricsWaitDuration(MONITORING_METRICS_DELAY_DURATION)
        .setMonitoringClient(monitoringClient);
  }

  public abstract Dataflow dataflowClient();

  public abstract Job job();

  public abstract MonitoringClient monitoringClient();

  public abstract Duration monitoringMetricsWaitDuration();

  private Dataflow.Projects.Locations.Jobs jobsClient() {
    return dataflowClient().projects().locations().jobs();
  }

  /**
   * Gets value of given metric.
   *
   * @param metricName - Name of the metric
   * @return metric value
   * @throws IOException
   */
  public Double getMetricValue(String metricName) throws IOException {
    LOG.info(
        "Getting '{}' metric for {} under {}", metricName, job().getId(), job().getProjectId());

    List<MetricUpdate> metrics =
        jobsClient()
            .getMetrics(job().getProjectId(), job().getLocation(), job().getId())
            .execute()
            .getMetrics();

    if (metrics == null) {
      LOG.warn("No metrics received for the job {} under {}.", job().getId(), job().getProjectId());
      return null;
    }

    for (MetricUpdate metricUpdate : metrics) {
      String currentMetricName = metricUpdate.getName().getName();
      String currentMetricOriginalName = metricUpdate.getName().getContext().get("original_name");
      if (Objects.equals(metricName, currentMetricName)
          || Objects.equals(metricName, currentMetricOriginalName)) {
        // only return if the metric is a scalar
        if (metricUpdate.getScalar() != null) {
          return ((Number) metricUpdate.getScalar()).doubleValue();
        } else {
          LOG.warn(
              "The given metric '{}' is not a scalar metric. Please use getMetrics instead.",
              metricName);
          return null;
        }
      }
    }
    LOG.warn(
        "Unable to find '{}' metric for {} under {}. Please check the metricName and try again!",
        metricName,
        job().getId(),
        job().getProjectId());
    return null;
  }

  /**
   * Since job is queried after completion ignore tentative and also any metrics associated with
   * each dataflow step.
   *
   * @param metricContext - Context associated with each metric
   * @return true if name is not in Deny listed contexts
   */
  private boolean isValidContext(Map<String, String> metricContext) {
    return Stream.of("tentative", "execution_step", "step").noneMatch(metricContext::containsKey);
  }

  /**
   * Check if the metrics are in valid names.
   *
   * @param metricInfo - Metric Information
   * @return true if name is not in Deny listed names
   */
  private boolean isValidNames(MetricStructuredName metricInfo) {
    return !(CURRENT_METRICS.matcher(metricInfo.getName()).find()
        || Stream.of("ElementCount", "MeanByteCount")
            .anyMatch(e -> metricInfo.getName().equals(e)));
  }

  /**
   * Calculate the average from a series.
   *
   * @param values the input series.
   * @return the averaged result.
   */
  private Double calculateAverage(List<Double> values) {
    return Math.round(values.stream().mapToDouble(d -> d).average().orElse(0.0) * scale) / scale;
  }

  /**
   * Calculate the maximum from a series.
   *
   * @param values the input series.
   * @return the averaged result.
   */
  private Double calculateMaximum(List<Double> values) {
    return Math.round(Collections.max(values) * scale) / scale;
  }

  /** Filter and Parse given metrics. */
  Map<String, Double> filterAndParseMetrics(List<MetricUpdate> metrics) {
    Map<String, Double> jobMetrics = new HashMap<>();
    for (MetricUpdate metricUpdate : metrics) {
      String metricName = metricUpdate.getName().getName();
      // Handle only scalar metrics.
      if (metricUpdate.getName().getOrigin().equals(DATAFLOW_SERVICEMETRICS_NAMESPACE)
          && isValidContext(metricUpdate.getName().getContext())
          && isValidNames(metricUpdate.getName())) {
        if (metricUpdate.getScalar() != null) {
          jobMetrics.put(metricName, ((Number) metricUpdate.getScalar()).doubleValue());
        } else if (metricUpdate.getDistribution() != null) {
          // currently, reporting distribution metrics as 4 separate scalar metrics
          ArrayMap distributionMap = (ArrayMap) metricUpdate.getDistribution();
          jobMetrics.put(
              metricName + "_COUNT", ((Number) distributionMap.get("count")).doubleValue());
          jobMetrics.put(metricName + "_MIN", ((Number) distributionMap.get("min")).doubleValue());
          jobMetrics.put(metricName + "_MAX", ((Number) distributionMap.get("max")).doubleValue());
          jobMetrics.put(metricName + "_SUM", ((Number) distributionMap.get("sum")).doubleValue());
        } else if (metricUpdate.getGauge() != null) {
          LOG.warn("Gauge metric {} cannot be handled.", metricName);
        }
      }
    }
    return jobMetrics;
  }

  /** Fetches the metrics for a given job id. */
  public Map<String, Double> getMetrics(String jobType, @Nullable ResourcePricing resourcePricing)
      throws IOException {
    LOG.info("Getting metrics for {} under {}", job().getId(), job().getProjectId());
    List<MetricUpdate> metrics =
        jobsClient()
            .getMetrics(job().getProjectId(), job().getLocation(), job().getId())
            .execute()
            .getMetrics();

    Map<String, Double> parsedMetrics = filterAndParseMetrics(metrics);
    LOG.info(
        "Sleeping for {} seconds to query Dataflow monitoring metrics.",
        monitoringMetricsWaitDuration().toSeconds());
    try {
      Thread.sleep(monitoringMetricsWaitDuration().toMillis());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    addComputedMetrics(parsedMetrics, jobType, resourcePricing);
    return parsedMetrics;
  }

  public void addComputedMetrics(
      Map<String, Double> metrics, String jobType, @Nullable ResourcePricing resourcePricing)
      throws IOException {
    Optional<TimeInterval> maybeWorkerTimerInterval;
    TimeInterval jobTimerInterval = null, workerTimerInterval = null, elapsedJobTimeInterval = null;
    try {
      maybeWorkerTimerInterval = getWorkerTimeInterval();
      jobTimerInterval = getJobTimeInterval();
      elapsedJobTimeInterval = getElapsedJobTimeInterval();
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }

    Optional<Double> elapsedTime =
        monitoringClient()
            .getJobElapsedTime(job().getProjectId(), job().getId(), elapsedJobTimeInterval);

    elapsedTime.ifPresent(elapsedTimeInsec -> metrics.put("TotalElapsedTimeSec", elapsedTimeInsec));

    if (maybeWorkerTimerInterval.isPresent()) {
      workerTimerInterval = maybeWorkerTimerInterval.get();
      metrics.put(
          "TotalRunTimeSec",
          (double)
              Timestamps.between(
                      workerTimerInterval.getStartTime(), workerTimerInterval.getEndTime())
                  .getSeconds());
    } else {
      workerTimerInterval = jobTimerInterval;
    }

    if (resourcePricing != null) {
      LOG.info("Estimating job cost..");
      double estimatedJobCost = resourcePricing.estimateJobCost(jobType, metrics);
      metrics.put("EstimatedJobCost", estimatedJobCost);
    }

    Optional<List<Double>> cpuUtilizationValues =
        monitoringClient()
            .getCpuUtilization(job().getProjectId(), job().getId(), workerTimerInterval);
    if (cpuUtilizationValues.isPresent()) {
      metrics.put("AvgCpuUtilization%", calculateAverage(cpuUtilizationValues.get()) * 100);
      metrics.put("MaxCpuUtilization%", calculateMaximum(cpuUtilizationValues.get()) * 100);
    }

    if (jobType.equals("JOB_TYPE_STREAMING")) {
      Optional<List<Double>> dataFreshnessValues =
          monitoringClient()
              .getDataFreshness(job().getProjectId(), job().getId(), workerTimerInterval);
      if (dataFreshnessValues.isPresent()) {
        metrics.put("AvgDataFreshnessSec", calculateAverage(dataFreshnessValues.get()));
        metrics.put("MaxDataFreshnessSec", calculateMaximum(dataFreshnessValues.get()));
      }

      Optional<List<Double>> systemLatencyValues =
          monitoringClient()
              .getSystemLatency(job().getProjectId(), job().getId(), workerTimerInterval);
      if (systemLatencyValues.isPresent()) {
        metrics.put("AvgSystemLatencySec", calculateAverage(systemLatencyValues.get()));
        metrics.put("MaxSystemLatencySec", calculateMaximum(systemLatencyValues.get()));
      }
    }
  }

  private List<JobMessage> listMessages(
      String project, String region, String jobId, String minimumImportance) {
    LOG.info("Listing messages of {} under {}", jobId, project);
    ListJobMessagesResponse response =
        Failsafe.with(RetryUtil.clientRetryPolicy())
            .get(
                () ->
                    jobsClient()
                        .messages()
                        .list(project, region, jobId)
                        .setMinimumImportance(minimumImportance)
                        .execute());
    List<JobMessage> messages = response.getJobMessages();
    LOG.info("Received {} messages for {} under {}", messages.size(), jobId, project);
    return messages;
  }

  /** Gets the time interval when workers were active to be used by monitoring client. */
  private Optional<TimeInterval> getWorkerTimeInterval() throws ParseException {
    TimeInterval.Builder builder = TimeInterval.newBuilder();
    List<JobMessage> messages =
        listMessages(
            job().getProjectId(), job().getLocation(), job().getId(), "JOB_MESSAGE_DETAILED");
    String startTime = null, endTime = null;

    for (JobMessage jobMessage : messages) {
      if (jobMessage.getMessageText() != null && !jobMessage.getMessageText().isEmpty()) {
        if (WORKER_START_PATTERN.matcher(jobMessage.getMessageText()).find()) {
          LOG.info("Found worker start message in job messages.");
          startTime = jobMessage.getTime();
        }
        if (WORKER_STOP_PATTERN.matcher(jobMessage.getMessageText()).find()) {
          LOG.info("Found worker stop message in job messages.");
          endTime = jobMessage.getTime();
        }
      }
    }

    if (startTime != null && endTime != null) {
      return Optional.of(
          builder
              .setStartTime(Timestamps.parse(startTime))
              .setEndTime(Timestamps.parse(endTime))
              .build());
    }
    return Optional.empty();
  }

  private TimeInterval getJobTimeInterval() throws ParseException {
    LOG.info("Parsing Job Timestamps for job {} under {}", job().getId(), job().getProjectId());

    return TimeInterval.newBuilder()
        .setStartTime(Timestamps.parse(job().getStartTime()))
        .setEndTime(Timestamps.parse(job().getCurrentStateTime()))
        .build();
  }

  private TimeInterval getElapsedJobTimeInterval() throws ParseException {
    return TimeInterval.newBuilder()
        .setStartTime(Timestamps.parse(job().getStartTime()))
        .setEndTime(Timestamps.fromMillis(System.currentTimeMillis()))
        .build();
  }

  /** Builder for {@link MetricsManager}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setDataflowClient(Dataflow value);

    public abstract Builder setJob(Job value);

    public abstract Builder setMonitoringClient(MonitoringClient value);

    public abstract Builder setMonitoringMetricsWaitDuration(Duration value);

    public abstract MetricsManager build();
  }
}
