terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  project = "toronto-ride-share-pipeline"
}

# resource "google_compute_network" "vpc_network" {
#   name = "terraform-network"
# }

resource "google_storage_bucket" "my_bucket" {
name     = "toronto-ride-share-files"
location = "northamerica-northeast2"
}