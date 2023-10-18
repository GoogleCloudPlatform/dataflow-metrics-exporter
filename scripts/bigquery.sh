#!/usr/bin/env bash
# Copyright 2023 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Substitute below variables
project_id=test-project
dataset_name=test-dataset
table_name=test-table


# Create dataset
bq --project_id ${project_id} mk ${dataset_name}

#Delete table (if exists)
bq --project_id ${project_id} rm -f -t ${dataset_name}.${table_name}

# Create table (from project root)
bq --project_id ${project_id} mk -t --description "Contains job run statistics" ${dataset_name}.${table_name} \
./metrics_bigquery_schema.json

# List tables in dataset
bq --project_id ${project_id} ls ${dataset_name}

# View table schema
bq --project_id ${project_id} show ${dataset_name}.${table_name}