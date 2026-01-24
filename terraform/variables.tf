variable "credentials" {
  description = "My Credentials"
  default     = "./secrets/terraform-runner.json" // works with file function main.tf, path from current dir
}


variable "project" {
  description = "Project"
  default     = "clear-variety-485314-u8"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "data_zoomcamp_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "clear-variety-485314-u8-terra-bucket" // must be globally unique
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}