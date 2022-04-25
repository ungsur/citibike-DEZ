The terraform in this directory creates:

Project: citibike-31437 Project,
Data Lake on google cloud storage: citibike_data_all 
Big Query Dataset: citibike_data_all

Populate the following parameters in the variables.tf file located in the terraform directory:
variable "project" {
  description = <project description>
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = <cloud location>
  type = string
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = <Dataset name>
}

Run the following commands from the terraform directory:

# Refresh service-account's auth-token for this session

gcloud auth application-default login

# Initialize state file (.tfstate)

terraform init

# Check changes to new infra plan

terraform plan -var="project=<your-gcp-project-id>"

# Create new infra

terraform apply -var="project=<your-gcp-project-id>"

# Delete infra after your work, to avoid costs on any running services

terraform destroy
