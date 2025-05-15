variable "project_id" {
  description = "Your GCP Project ID"
  type        = string
}

variable "region" {
  description = "Region for GCP resources"
  type        = string
  default     = "asia-southeast1"
}

variable "zone" {
  description = "Zone for GCP resources"
  type        = string
  default     = "asia-southeast1-a"
} 