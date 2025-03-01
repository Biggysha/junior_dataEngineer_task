terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.project_id
  region = "asia-southeast1"
  credentials = file(var.credentials)
}

resource "google_storage_bucket" "raw_data" {
  name     = "my-raw-data-bucket"
  location = "asia-southeast1"
  force_destroy = true
}

resource "google_storage_bucket" "process_data" {
  name     = "my-process-data-bucket"
  location = "asia-southeast1"
  force_destroy = true
}

resource "google_sql_database_instance" "postgres" {
  name             = "process-table1"
  database_version = "POSTGRES_14"
  region           = "asia-southeast1"
  deletion_protection = false
  settings {
    tier = "db-f1-micro"
    deletion_protection_enabled = false
  }
}