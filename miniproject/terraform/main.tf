terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

# Configure the Google Cloud provider
provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# Create a GCS bucket
resource "google_storage_bucket" "data_lake_bucket" {
  name          = "${var.project_id}-data-lake"
  location      = var.region
  force_destroy = true

  storage_class = "STANDARD"
  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30  # days
    }
    action {
      type = "Delete"
    }
  }
}

# Create a Compute Engine instance
resource "google_compute_instance" "vm_instance" {
  name         = "data-processing-instance"
  machine_type = "e2-medium"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network = "default"
    access_config {
      // Ephemeral public IP
    }
  }

  metadata_startup_script = "echo hi > /test.txt"

  tags = ["data-processing"]
}

# Create BigQuery Dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = "data_warehouse"
  friendly_name              = "Data Warehouse"
  description                = "Dataset for data warehouse"
  location                   = var.region
  delete_contents_on_destroy = true

  labels = {
    environment = "development"
  }
}

# Create a sample BigQuery table
resource "google_bigquery_table" "table" {
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = "sample_table"

  time_partitioning {
    type = "DAY"
  }

  labels = {
    environment = "development"
  }

  schema = <<EOF
[
  {
    "name": "id",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "created_at",
    "type": "TIMESTAMP",
    "mode": "REQUIRED"
  }
]
EOF
}
