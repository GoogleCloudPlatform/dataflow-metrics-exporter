#!/bin/bash

# Set the project ID and region
project_id="ihaffa-sandbox"  # Replace with your project ID
region="us-central1"          # Replace with your region (e.g., us-central1)

# Get the list of running and completed Dataflow job IDs
job_ids=$(gcloud dataflow jobs list \
  --project="$project_id" \
  --region="$region" \
  --filter="state=DONE" \
  --format="value(id)" \
  --quiet | tr '\n' ',')

# Remove the trailing comma if there are any job IDs
echo $job_ids
job_ids="${job_ids%,}"

# Print the job IDs (optional)
echo "Dataflow Job IDs (running and completed):"
echo $job_ids_string

# You can now use the $job_ids_string variable in your script
# For example, you can iterate through the job IDs:
if [ -n "$job_ids_string" ]; then
  IFS=',' read -r -a job_id_array <<< "$job_ids_string"
  for job_id in "${job_id_array[@]}"; do
    echo "Processing job ID: $job_id"
    # Add your logic here to process each job ID
  done
else
  echo "No running or completed Dataflow jobs found."
fi

json_file="/usr/local/google/home/ihaffa/sce/personal/dataflow-metrics-exporter/src/main/resources/df-job.json"
updated_json=$(jq --arg job_id "$job_ids" '.jobId = $job_id' "$json_file")

# Write the updated JSON back to the file
echo "$updated_json" > "$json_file"

echo "Job ID in $json_file updated to: $job_id"


