resource "azurerm_container_app_job" "hourly_etl" {

  name                = "weather-etl-hourly"
  resource_group_name = "portfolio-rg"
  location            = "Poland Central"

  container_app_environment_id = data.azurerm_container_app_environment.main.id


  replica_timeout_in_seconds = 1800
  replica_retry_limit        = 1


  schedule_trigger_config {

    cron_expression = "7 * * * *"

    parallelism = 1

    replica_completion_count = 1
  }


  identity {

    type = "UserAssigned"

    identity_ids = [
      azurerm_user_assigned_identity.etl_jobs.id
    ]
  }


  template {

    container {

      name  = "weather-etl"
      image = "pchelkovskiy/weather-data-platform:latest"

      cpu    = 0.5
      memory = "1Gi"


      env {

        name  = "ENVIRONMENT"
        value = var.environment
      }
    }
  }


  tags = {

    environment = var.environment
    project     = var.project_name
  }
}