variable "credentials" {
  description = "My Credentials"
  default     = "/home/adaora/terra/keys/my-creds.json"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "project" {
  description = "Project"
  default     = "adaora"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "ny_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "adaoraah-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}