variable "project_id" {
  description = "The ID of the GCP project"
  type        = string
}

variable "credentials" {
  description = "The path to the GCP credentials file"
  type        = string
  default     = "burmese-ai6666-54ed5333f7c9.json"
}