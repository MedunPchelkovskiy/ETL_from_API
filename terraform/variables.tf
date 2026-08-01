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


# Non-secret Azure identifiers

variable "tenant_id" {
  description = "Azure AD tenant ID"
  type        = string
  default     = ""
}

variable "client_id" {
  description = "Azure service principal client ID"
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
  type    = string
  default = "weather-etl"
}


variable "location" {
  type    = string
  default = "germanywestcentral"
}


variable "environment" {
  type    = string
  default = "prod"
}


variable "pushgateway_image" {
  type    = string
  default = "prom/pushgateway:latest"
}


variable "pushgateway_cpu" {
  type    = number
  default = 0.25
}


variable "pushgateway_memory" {
  type    = string
  default = "0.5Gi"
}
