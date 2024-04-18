terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "5.24.0"
    }
  }
}

provider "google" {
  project = "toronto-ride-share-pipeline"
}

resource "google_compute_network" "dataproc_network" {
  name = "dataproc-network"
}

# Define the subnet within the VPC network
resource "google_compute_subnetwork" "dataproc_sub_net" {
  name          = "dataproc-subnet"
  ip_cidr_range = "10.0.1.0/24"
  region        = "northamerica-northeast2"
  network       = google_compute_network.dataproc_network.self_link
}

resource "google_storage_bucket" "my_bucket" {
name     = "toronto-ride-share-files"
location = "northamerica-northeast2"
force_destroy = true
}

resource "google_storage_bucket_object" "rides" {
  name          = "rides/"
  bucket        = google_storage_bucket.my_bucket.name
  content = ""
}

resource "google_storage_bucket_object" "weather" {
  name          = "weather/"
  bucket        = google_storage_bucket.my_bucket.name
  content = ""
}

resource "google_bigquery_dataset" "toronto_ride_share_dataset" {
  dataset_id                  = "ride_share"
  friendly_name               = "ride share"
  location                    = "US"
  
}



resource "google_dataproc_cluster" "my_cluster" {
  name          = "cluster-data-proc-afnan"
  region        = "northamerica-northeast2"
  

  cluster_config {
    
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-2"
      
      disk_config {
        boot_disk_type   = "pd-balanced"
        boot_disk_size_gb = 30
        
      }
      
    }
    worker_config {
      num_instances = 0
    }

    software_config {
      image_version = "2.2-debian12"
      
      
    }
  gce_cluster_config {
      internal_ip_only = true
      subnetwork       = google_compute_subnetwork.dataproc_sub_net.self_link
    }

  }
  
  
}

