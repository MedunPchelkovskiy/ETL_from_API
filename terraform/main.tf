data "azurerm_client_config" "current" {}

data "azurerm_container_app_environment" "main" {
  name                = "portfolio-env"
  resource_group_name = "portfolio-rg"
}


resource "azurerm_resource_group" "main" {
  name     = "${var.project_name}-rg"
  location = var.location
}


resource "azurerm_log_analytics_workspace" "main" {
  name                = "${var.project_name}-logs"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  sku               = "PerGB2018"
  retention_in_days = 30
}


resource "azurerm_container_app" "pushgateway" {
  name                         = "${var.project_name}-pushgateway"
  resource_group_name          = "portfolio-rg"
  container_app_environment_id = data.azurerm_container_app_environment.main.id

  revision_mode = "Single"


  template {
    min_replicas = 1
    max_replicas = 1

    container {
      name   = "pushgateway"
      image  = var.pushgateway_image
      cpu    = var.pushgateway_cpu
      memory = var.pushgateway_memory
    }
  }


  ingress {
    external_enabled = false

    target_port = 9091
    transport   = "http"

    traffic_weight {
      percentage      = 100
      latest_revision = true
    }
  }


  tags = {
    environment = var.environment
    project     = var.project_name
  }
}