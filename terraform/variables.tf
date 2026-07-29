variable "project_name" {
  description = "Short name used as prefix for all resources"
  type        = string
  default     = "weather-etl"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "westeurope" # pick the region closest/cheapest for you
}

variable "environment" {
  description = "Environment tag (dev/prod)"
  type        = string
  default     = "prod"
}

variable "pushgateway_image" {
  description = "Pushgateway container image"
  type        = string
  default     = "prom/pushgateway:latest"
}

variable "pushgateway_cpu" {
  description = "vCPU allocated to the Pushgateway container"
  type        = number
  default     = 0.25
}

variable "pushgateway_memory" {
  description = "Memory allocated to the Pushgateway container (e.g. 0.5Gi)"
  type        = string
  default     = "0.5Gi"
}
