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
package com.google.cloud.dfmetrics.model;

import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.dataflow.Dataflow;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auto.value.AutoValue;
import com.google.cloud.dfmetrics.utils.MetricsCollectorUtils;

/** Class {@link DataflowClient} encapsulates the client instance to interact with Dataflow API. */
@AutoValue
public class DataflowClient {
  private static final String APPLICATION_NAME = "google-pso-tool/dataflow-metrics-exporter";

  public static Builder builder() {
    return new AutoValue_DataflowClient.Builder();
  }

  /** Builder for {@link DataflowClient}. */
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

    public Dataflow build() {
      Credentials credentials =
          getCredentials() != null ? getCredentials() : MetricsCollectorUtils.googleCredentials();
      return new Dataflow.Builder(
              Utils.getDefaultTransport(),
              Utils.getDefaultJsonFactory(),
              new HttpCredentialsAdapter(credentials))
          .setApplicationName(APPLICATION_NAME)
          .build();
    }
  }
}
