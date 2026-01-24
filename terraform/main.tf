terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  credentials = "/workspaces/data-engineering-zoomcamp/terraform/secrets/terraform-runner.json"
  project = "clear-variety-485314-u8"
  region  = "us-central1"
}

resource "google_storage_bucket" "data-lake-bucket" {
  name          = "clear-variety-485314-u8-terra-bucket"
  location      = "US"

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}
