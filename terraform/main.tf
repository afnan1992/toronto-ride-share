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

# resource "google_compute_network" "vpc_network" {
#   name = "terraform-network"
# }

resource "google_storage_bucket" "my_bucket" {
name     = "toronto-ride-share-files"
location = "northamerica-northeast2"
force_destroy = true
}

resource "google_storage_bucket_object" "rides" {
  name          = "rides/"
 
  bucket        = "toronto-ride-share-files"
}

resource "google_storage_bucket_object" "weather" {
  name          = "weather/"
 
  bucket        = "toronto-ride-share-files"
}

resource "google_bigquery_dataset" "toronto_ride_share_dataset" {
  dataset_id                  = "ride_share"
  friendly_name               = "ride share"
  location                    = "US"
  
}



# resource "google_dataproc_cluster" "my_cluster" {
#   name          = "cluster-data-proc-afnan"
#   region        = "northamerica-northeast2"
  

#   cluster_config {
    
#     master_config {
#       num_instances = 1
#       machine_type  = "n1-standard-2"
      
#       disk_config {
#         boot_disk_type   = "pd-balanced"
#         boot_disk_size_gb = 30
        
#       }
      
#     }
#     software_config {
#       image_version = "2.2-debian12"
      
      
#     }
#   gce_cluster_config {
#       internal_ip_only = false
#     }

#   }
  
  
# }

