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

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.cloud.dfmetrics.utils.MetricsCollectorUtils;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.cloud.monitoring.v3.MetricServiceClient.ListTimeSeriesPagedResponse;
import com.google.cloud.monitoring.v3.MetricServiceSettings;
import com.google.monitoring.v3.Aggregation;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.Point;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.monitoring.v3.TimeSeries;
import com.google.protobuf.Duration;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client for interacting with Google Cloud Monitoring. */
public final class MonitoringClient {
  private static final Logger LOG = LoggerFactory.getLogger(MonitoringClient.class);

  private static final Duration DEFAULT_ALIGNMENT_PERIOD =
      Duration.newBuilder().setSeconds(60).build();

  private final MetricServiceClient metricServiceClient;

  private MonitoringClient(CredentialsProvider credentialsProvider) throws IOException {
    MetricServiceSettings metricServiceSettings =
        MetricServiceSettings.newBuilder().setCredentialsProvider(credentialsProvider).build();
    this.metricServiceClient = MetricServiceClient.create(metricServiceSettings);
  }

  private MonitoringClient(MetricServiceClient metricServiceClient) {
    this.metricServiceClient = metricServiceClient;
  }

  public static MonitoringClient withMonitoringClient(MetricServiceClient metricServiceClient) {
    return new MonitoringClient(metricServiceClient);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link MonitoringClient}. */
  public static final class Builder {
    private Credentials credentials;

    private Builder() {}

    public Credentials getCredentials() {
      return credentials;
    }

    public Builder setCredentials(Credentials value) {
      credentials = value;
      return this;
    }

    public MonitoringClient build() throws IOException {
      Credentials credentials =
          getCredentials() != null ? getCredentials() : MetricsCollectorUtils.googleCredentials();
      return new MonitoringClient(FixedCredentialsProvider.create(credentials));
    }
  }

  /**
   * Extracts Values from time series.
   *
   * @param response
   * @return
   */
  private List<Double> extractValuesFromTimeSeriesAsDouble(ListTimeSeriesPagedResponse response) {
    List<Double> values = new ArrayList<>();
    for (TimeSeries ts : response.iterateAll()) {
      for (Point point : ts.getPointsList()) {
        values.add(point.getValue().getDoubleValue());
      }
    }
    return values;
  }

  /**
   * Lists time series that match a filter.
   *
   * @param request time series request to execute
   * @return list of Double values of time series
   */
  public List<Double> listTimeSeriesAsDouble(ListTimeSeriesRequest request) {
    return extractValuesFromTimeSeriesAsDouble(metricServiceClient.listTimeSeries(request));
  }

  /**
   * Creates a Time Series request.
   *
   * @param project
   * @param filter
   * @param timeInterval
   * @param aggregation
   * @return
   */
  private ListTimeSeriesRequest getTimeSeriesRequest(
      String project, String filter, TimeInterval timeInterval, Aggregation aggregation) {
    return ListTimeSeriesRequest.newBuilder()
        .setName(ProjectName.of(project).toString())
        .setFilter(filter)
        .setInterval(timeInterval)
        .setAggregation(aggregation)
        .build();
  }

  private Aggregation getAggregation() {
    return getAggregation(
        DEFAULT_ALIGNMENT_PERIOD, Aggregation.Aligner.ALIGN_MEAN, Aggregation.Reducer.REDUCE_MAX);
  }

  private Aggregation getAggregation(
      Duration alignmentPeriod, Aggregation.Aligner aligner, Aggregation.Reducer reducer) {
    return Aggregation.newBuilder()
        .setAlignmentPeriod(alignmentPeriod)
        .setPerSeriesAligner(aligner)
        .setCrossSeriesReducer(reducer)
        .build();
  }

  /**
   * Get elapsed time for a job.
   *
   * @param project the project that the job is running under
   * @param jobId dataflow job id
   * @return elapsed time
   * @throws ParseException if timestamp is inaccurate
   */
  public Optional<Double> getJobElapsedTime(
      String project, String jobId, TimeInterval timeInterval) {
    LOG.info("Getting elapsed time for {} under {}", jobId, project);
    String filter =
        String.format(
            "metric.type = \"dataflow.googleapis.com/job/elapsed_time\" "
                + "AND metric.labels.job_id=\"%s\" ",
            jobId);
    ListTimeSeriesRequest request =
        getTimeSeriesRequest(project, filter, timeInterval, getAggregation());
    List<Double> timeSeries = listTimeSeriesAsDouble(request);
    if (timeSeries.isEmpty()) {
      LOG.warn("No monitoring data found. Unable to get Data freshness information.");
      return Optional.empty();
    }
    return Optional.of(Collections.max(timeSeries));
  }

  /**
   * Gets the Data freshness time series data for a given Job.
   *
   * @param project the project that the job is running under
   * @param jobId dataflow job id
   * @param timeInterval interval for the monitoring query
   * @return Data freshness time series data for the given job.
   */
  public Optional<List<Double>> getDataFreshness(
      String project, String jobId, TimeInterval timeInterval) {
    LOG.info("Getting data freshness for {} under {}", jobId, project);
    String filter =
        String.format(
            "metric.type = \"dataflow.googleapis.com/job/per_stage_data_watermark_age\" "
                + "AND metric.labels.job_id = \"%s\"",
            jobId);
    ListTimeSeriesRequest request =
        getTimeSeriesRequest(project, filter, timeInterval, getAggregation());
    List<Double> timeSeries = listTimeSeriesAsDouble(request);
    if (timeSeries.isEmpty()) {
      LOG.warn("No monitoring data found. Unable to get Data freshness information.");
      return Optional.empty();
    }
    return Optional.of(timeSeries);
  }

  /**
   * Gets the System Latency time series data for a given Job.
   *
   * @param project the project that the job is running under
   * @param jobId dataflow job id
   * @param timeInterval interval for the monitoring query
   * @return System Latency time series data for the given job.
   */
  public Optional<List<Double>> getSystemLatency(
      String project, String jobId, TimeInterval timeInterval) {
    LOG.info("Getting system latency for {} under {}", jobId, project);
    String filter =
        String.format(
            "metric.type = \"dataflow.googleapis.com/job/per_stage_system_lag\" "
                + "AND metric.labels.job_id = \"%s\"",
            jobId);
    ListTimeSeriesRequest request =
        getTimeSeriesRequest(project, filter, timeInterval, getAggregation());
    List<Double> timeSeries = listTimeSeriesAsDouble(request);
    if (timeSeries.isEmpty()) {
      LOG.warn("No monitoring data found. Unable to get System Latency information.");
      return Optional.empty();
    }
    return Optional.of(timeSeries);
  }

  /**
   * Gets the CPU Utilization time series data for a given Job.
   *
   * @param project the project that the job is running under
   * @param jobId dataflow job id
   * @param timeInterval interval for the monitoring query
   * @return CPU Utilization time series data for the given job.
   */
  public Optional<List<Double>> getCpuUtilization(
      String project, String jobId, TimeInterval timeInterval) {
    LOG.info("Getting CPU utilization for {} under {}", jobId, project);
    String filter =
        String.format(
            "metric.type = \"compute.googleapis.com/instance/cpu/utilization\" "
                + "AND resource.labels.project_id = \"%s\" "
                + "AND metadata.user_labels.dataflow_job_id = \"%s\"",
            project, jobId);
    Aggregation aggregation =
        Aggregation.newBuilder()
            .setAlignmentPeriod(DEFAULT_ALIGNMENT_PERIOD)
            .setPerSeriesAligner(Aggregation.Aligner.ALIGN_MEAN)
            .setCrossSeriesReducer(Aggregation.Reducer.REDUCE_MEAN)
            .addGroupByFields("resource.instance_id")
            .build();
    ListTimeSeriesRequest request =
        getTimeSeriesRequest(project, filter, timeInterval, aggregation);
    ListTimeSeriesRequest.newBuilder()
        .setName(ProjectName.of(project).toString())
        .setFilter(filter)
        .setInterval(timeInterval)
        .setAggregation(aggregation)
        .build();
    List<Double> timeSeries = listTimeSeriesAsDouble(request);
    if (timeSeries.isEmpty()) {
      LOG.warn("No monitoring data found. Unable to get CPU utilization information.");
      return Optional.empty();
    }
    return Optional.of(timeSeries);
  }
}
