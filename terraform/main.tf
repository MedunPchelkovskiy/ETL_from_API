resource "azurerm_resource_group" "main" {
  name     = "${var.project_name}-rg"
  location = var.location
}

# Container Apps Environment requires a Log Analytics workspace behind it
# (even if you don't actively use its logs) — this is the minimum billed
# for that; keep retention short to control cost.
resource "azurerm_log_analytics_workspace" "main" {
  name                = "${var.project_name}-logs"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days    = 30
}

resource "azurerm_container_app_environment" "main" {
  name                       = "${var.project_name}-env"
  location                   = azurerm_resource_group.main.location
  resource_group_name        = azurerm_resource_group.main.name
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id
}

# Always-on Pushgateway — the only piece in this stack that runs continuously
# (min_replicas = 1). Everything else (the ETL itself) will be Container
# Apps Jobs, which scale to zero and only bill while actually running.
resource "azurerm_container_app" "pushgateway" {
  name                         = "${var.project_name}-pushgateway"
  container_app_environment_id = azurerm_container_app_environment.main.id
  resource_group_name          = azurerm_resource_group.main.name
  revision_mode                = "Single"

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
    external_enabled = false # only reachable from inside the Container Apps env
    target_port      = 9091
    transport         = "http"

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
