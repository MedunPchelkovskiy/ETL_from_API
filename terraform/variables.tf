# Non-sensitive config values — these go directly as plain environment
# variables on the Container Apps Jobs later, NOT into Key Vault. Add the
# real values to terraform.tfvars (already gitignored, but these aren't
# sensitive enough to require Key Vault either way).

variable "db_host" {
  type    = string
  default = ""
}

variable "db_port" {
  type    = string
  default = "5432"
}

variable "db_name_scraped" {
  type    = string
  default = ""
}

variable "db_name_raw" {
  type    = string
  default = ""
}

variable "db_name_transformed" {
  type    = string
  default = ""
}

variable "tenant_id" {
  description = "Azure AD tenant ID — an identifier, not a secret"
  type        = string
  default     = ""
}

variable "client_id" {
  description = "Azure service principal app ID — an identifier, not a secret"
  type        = string
  default     = ""
}

variable "account_url" {
  type    = string
  default = "https://youraccount.dfs.core.windows.net"
}

variable "file_system" {
  type    = string
  default = ""
}

variable "prefect_api_url" {
  type    = string
  default = ""
}

variable "project_dir" {
  type    = string
  default = ""
}

# BASE_DIR_* paths — static, safe to keep as defaults; override in tfvars
# only if you rename the Lakehouse folder structure.
variable "base_dirs" {
  type = map(string)
  default = {
    RAW                = "MyLakehouse/Meteo/raw"
    SILVER             = "MyLakehouse/Meteo/silver"
    GOLD               = "MyLakehouse/Meteo/gold/daily-forecast"
    FIVE_DAY_GOLD      = "MyLakehouse/Meteo/gold/five-day-forecast"
    DAILY_SUMM_GOLD    = "MyLakehouse/Meteo/gold/daily-summ-forecast"
    WEEKLY_SUMM_GOLD   = "MyLakehouse/Meteo/gold/weekly-summ-forecast"
    MONTHLY_SUMM_GOLD  = "MyLakehouse/Meteo/gold/monthly-summ-forecast"
    YEARLY_SUMM_GOLD   = "MyLakehouse/Meteo/gold/yearly-summ-forecast"
    SEASONAL_SUMM_GOLD = "MyLakehouse/Meteo/gold/seasonally-summ-forecast"
  }
}

variable "project_name" {
  description = "Short name used as prefix for all resources"
  type        = string
  default     = "weather-etl"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "germanywestcentral" # pick the region closest/cheapest for you
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


# Reference list only (not a resource) — the secret names the push script
# will create in Key Vault, based on .env.secrets.example. Used when wiring
# secretRef entries into the 5 Container Apps Jobs later so the names match
# exactly what's actually in the vault.
locals {
  key_vault_secret_names = [
    "db-user",
    "db-password",
    "db-conn-raw",
    "foreca-api-key",
    "accuweather-api-key",
    "meteoblue-api-key",
    "weatherbit-api-key",
    "tommorow-api-key",
    "openweathermap-api-key",
    "weatherapi-api-key",
    "client-secret",
  ]
}
