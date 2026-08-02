resource "azurerm_container_app_job" "hourly_etl" {

  name                = "weather-etl-hourly"
  resource_group_name = "portfolio-rg"
  location            = "Poland Central"

  container_app_environment_id = data.azurerm_container_app_environment.main.id

  replica_timeout_in_seconds = 1800
  replica_retry_limit        = 1


  schedule_trigger_config {
    cron_expression          = "7 * * * *"
    parallelism              = 1
    replica_completion_count = 1
  }


  identity {
    type = "UserAssigned"

    identity_ids = [
      azurerm_user_assigned_identity.etl_jobs.id
    ]
  }


  secret {
    name                = "client-secret"
    key_vault_secret_id = data.azurerm_key_vault_secret.client_secret.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "db-user"
    key_vault_secret_id = data.azurerm_key_vault_secret.db_user.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "db-password"
    key_vault_secret_id = data.azurerm_key_vault_secret.db_password.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "db-conn-raw"
    key_vault_secret_id = data.azurerm_key_vault_secret.db_conn_raw.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "supabase-db-url"
    key_vault_secret_id = data.azurerm_key_vault_secret.supabase_db_url.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "foreca-api-key"
    key_vault_secret_id = data.azurerm_key_vault_secret.foreca_api_key.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "accuweather-api-key"
    key_vault_secret_id = data.azurerm_key_vault_secret.accuweather_api_key.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "meteoblue-api-key"
    key_vault_secret_id = data.azurerm_key_vault_secret.meteoblue_api_key.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "weatherbit-api-key"
    key_vault_secret_id = data.azurerm_key_vault_secret.weatherbit_api_key.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "tommorow-api-key"
    key_vault_secret_id = data.azurerm_key_vault_secret.tommorow_api_key.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "openweathermap-api-key"
    key_vault_secret_id = data.azurerm_key_vault_secret.openweathermap_api_key.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "weatherapi-api-key"
    key_vault_secret_id = data.azurerm_key_vault_secret.weatherapi_api_key.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "prefect-api-key"
    key_vault_secret_id = data.azurerm_key_vault_secret.prefect_api_key.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
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

      env {
        name  = "DB_HOST"
        value = var.db_host
      }

      env {
        name  = "DB_PORT"
        value = var.db_port
      }

      env {
        name  = "DB_NAME_FOR_SCRAPED_WEATHER_DATA"
        value = var.db_name_scraped
      }

      env {
        name  = "DB_NAME_FOR_RAW_WEATHER_API_DATA"
        value = var.db_name_raw
      }

      env {
        name  = "DB_NAME_FOR_TRANSFORMED_WEATHER_API_DATA"
        value = var.db_name_transformed
      }

      env {
        name  = "TENANT_ID"
        value = var.tenant_id
      }

      env {
        name  = "CLIENT_ID"
        value = var.client_id
      }

      env {
        name  = "ACCOUNT_URL"
        value = var.account_url
      }

      env {
        name  = "FILE_SYSTEM"
        value = var.file_system
      }

      env {
        name  = "PREFECT_API_URL"
        value = var.prefect_api_url
      }

      env {
        name  = "PROJECT_DIR"
        value = var.project_dir
      }


      env {
        name        = "DB_USER"
        secret_name = "db-user"
      }

      env {
        name        = "DB_PASSWORD"
        secret_name = "db-password"
      }

      env {
        name        = "DB_CONN_RAW"
        secret_name = "db-conn-raw"
      }

      env {
        name        = "SUPABASE_DB_URL"
        secret_name = "supabase-db-url"
      }

      env {
        name        = "CLIENT_SECRET"
        secret_name = "client-secret"
      }

      env {
        name        = "FORECA_API_KEY"
        secret_name = "foreca-api-key"
      }

      env {
        name        = "ACCUWEATHER_API_KEY"
        secret_name = "accuweather-api-key"
      }

      env {
        name        = "METEOBLUE_API_KEY"
        secret_name = "meteoblue-api-key"
      }

      env {
        name        = "WEATHERBIT_API_KEY"
        secret_name = "weatherbit-api-key"
      }

      env {
        name        = "TOMMOROW_API_KEY"
        secret_name = "tommorow-api-key"
      }

      env {
        name        = "OPENWEATHERMAP_API_KEY"
        secret_name = "openweathermap-api-key"
      }

      env {
        name        = "WEATHERAPI_API_KEY"
        secret_name = "weatherapi-api-key"
      }

      env {
        name        = "PREFECT_API_KEY"
        secret_name = "prefect-api-key"
      }

      env {
        name  = "BASE_DIR_RAW"
        value = "MyLakehouse/Meteo/raw"
      }

      env {
        name  = "BASE_DIR_SILVER"
        value = "MyLakehouse/Meteo/silver"
      }

      env {
        name  = "BASE_DIR_GOLD"
        value = "MyLakehouse/Meteo/gold/daily-forecast"
      }

      env {
        name  = "BASE_DIR_FIVE_DAY_GOLD"
        value = "MyLakehouse/Meteo/gold/five-day-forecast"
      }

      env {
        name  = "BASE_DIR_DAILY_SUMM_GOLD"
        value = "MyLakehouse/Meteo/gold/daily-summ-forecast"
      }

      env {
        name  = "BASE_DIR_WEEKLY_SUMM_GOLD"
        value = "MyLakehouse/Meteo/gold/weekly-summ-forecast"
      }

      env {
        name  = "BASE_DIR_MONTHLY_SUMM_GOLD"
        value = "MyLakehouse/Meteo/gold/monthly-summ-forecast"
      }

      env {
        name  = "BASE_DIR_YEARLY_SUMM_GOLD"
        value = "MyLakehouse/Meteo/gold/yearly-summ-forecast"
      }

      env {
        name  = "BASE_DIR_SEASONALLY_SUMM_GOLD"
        value = "MyLakehouse/Meteo/gold/seasonally-summ-forecast"
      }
    }
  }


  tags = {
    environment = var.environment
    project     = var.project_name
  }
}


resource "azurerm_container_app_job" "gold_daily_summ_flow" {

  name                = "weather-etl-daily"
  resource_group_name = "portfolio-rg"
  location            = "Poland Central"

  container_app_environment_id = data.azurerm_container_app_environment.main.id

  replica_timeout_in_seconds = 1800
  replica_retry_limit        = 1


  schedule_trigger_config {
    cron_expression          = "5 0 * * *"
    parallelism              = 1
    replica_completion_count = 1
  }


  identity {
    type = "UserAssigned"

    identity_ids = [
      azurerm_user_assigned_identity.etl_jobs.id
    ]
  }


  secret {
    name                = "client-secret"
    key_vault_secret_id = data.azurerm_key_vault_secret.client_secret.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "db-user"
    key_vault_secret_id = data.azurerm_key_vault_secret.db_user.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "db-password"
    key_vault_secret_id = data.azurerm_key_vault_secret.db_password.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "db-conn-raw"
    key_vault_secret_id = data.azurerm_key_vault_secret.db_conn_raw.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "supabase-db-url"
    key_vault_secret_id = data.azurerm_key_vault_secret.supabase_db_url.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "foreca-api-key"
    key_vault_secret_id = data.azurerm_key_vault_secret.foreca_api_key.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "accuweather-api-key"
    key_vault_secret_id = data.azurerm_key_vault_secret.accuweather_api_key.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "meteoblue-api-key"
    key_vault_secret_id = data.azurerm_key_vault_secret.meteoblue_api_key.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "weatherbit-api-key"
    key_vault_secret_id = data.azurerm_key_vault_secret.weatherbit_api_key.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "tommorow-api-key"
    key_vault_secret_id = data.azurerm_key_vault_secret.tommorow_api_key.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "openweathermap-api-key"
    key_vault_secret_id = data.azurerm_key_vault_secret.openweathermap_api_key.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "weatherapi-api-key"
    key_vault_secret_id = data.azurerm_key_vault_secret.weatherapi_api_key.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }

  secret {
    name                = "prefect-api-key"
    key_vault_secret_id = data.azurerm_key_vault_secret.prefect_api_key.versionless_id
    identity            = azurerm_user_assigned_identity.etl_jobs.id
  }


  template {

    container {

      name  = "weather-etl-daily"
      image = "pchelkovskiy/weather-data-platform:latest"

      cpu    = 0.5
      memory = "1Gi"

      command = [
        "python",
        "src/flows/gold/daily_summ_agg_data.py"
      ]


      env {
        name  = "ENVIRONMENT"
        value = var.environment
      }

      env {
        name  = "DB_HOST"
        value = var.db_host
      }

      env {
        name  = "DB_PORT"
        value = var.db_port
      }

      env {
        name  = "DB_NAME_FOR_SCRAPED_WEATHER_DATA"
        value = var.db_name_scraped
      }

      env {
        name  = "DB_NAME_FOR_RAW_WEATHER_API_DATA"
        value = var.db_name_raw
      }

      env {
        name  = "DB_NAME_FOR_TRANSFORMED_WEATHER_API_DATA"
        value = var.db_name_transformed
      }

      env {
        name  = "TENANT_ID"
        value = var.tenant_id
      }

      env {
        name  = "CLIENT_ID"
        value = var.client_id
      }

      env {
        name  = "ACCOUNT_URL"
        value = var.account_url
      }

      env {
        name  = "FILE_SYSTEM"
        value = var.file_system
      }

      env {
        name  = "PREFECT_API_URL"
        value = var.prefect_api_url
      }

      env {
        name  = "PROJECT_DIR"
        value = var.project_dir
      }


      env {
        name        = "DB_USER"
        secret_name = "db-user"
      }

      env {
        name        = "DB_PASSWORD"
        secret_name = "db-password"
      }

      env {
        name        = "DB_CONN_RAW"
        secret_name = "db-conn-raw"
      }

      env {
        name        = "SUPABASE_DB_URL"
        secret_name = "supabase-db-url"
      }

      env {
        name        = "CLIENT_SECRET"
        secret_name = "client-secret"
      }

      env {
        name        = "FORECA_API_KEY"
        secret_name = "foreca-api-key"
      }

      env {
        name        = "ACCUWEATHER_API_KEY"
        secret_name = "accuweather-api-key"
      }

      env {
        name        = "METEOBLUE_API_KEY"
        secret_name = "meteoblue-api-key"
      }

      env {
        name        = "WEATHERBIT_API_KEY"
        secret_name = "weatherbit-api-key"
      }

      env {
        name        = "TOMMOROW_API_KEY"
        secret_name = "tommorow-api-key"
      }

      env {
        name        = "OPENWEATHERMAP_API_KEY"
        secret_name = "openweathermap-api-key"
      }

      env {
        name        = "WEATHERAPI_API_KEY"
        secret_name = "weatherapi-api-key"
      }

      env {
        name        = "PREFECT_API_KEY"
        secret_name = "prefect-api-key"
      }

      env {
        name  = "BASE_DIR_RAW"
        value = "MyLakehouse/Meteo/raw"
      }

      env {
        name  = "BASE_DIR_SILVER"
        value = "MyLakehouse/Meteo/silver"
      }

      env {
        name  = "BASE_DIR_GOLD"
        value = "MyLakehouse/Meteo/gold/daily-forecast"
      }

      env {
        name  = "BASE_DIR_FIVE_DAY_GOLD"
        value = "MyLakehouse/Meteo/gold/five-day-forecast"
      }

      env {
        name  = "BASE_DIR_DAILY_SUMM_GOLD"
        value = "MyLakehouse/Meteo/gold/daily-summ-forecast"
      }

      env {
        name  = "BASE_DIR_WEEKLY_SUMM_GOLD"
        value = "MyLakehouse/Meteo/gold/weekly-summ-forecast"
      }

      env {
        name  = "BASE_DIR_MONTHLY_SUMM_GOLD"
        value = "MyLakehouse/Meteo/gold/monthly-summ-forecast"
      }

      env {
        name  = "BASE_DIR_YEARLY_SUMM_GOLD"
        value = "MyLakehouse/Meteo/gold/yearly-summ-forecast"
      }

      env {
        name  = "BASE_DIR_SEASONALLY_SUMM_GOLD"
        value = "MyLakehouse/Meteo/gold/seasonally-summ-forecast"
      }
    }
  }


  tags = {
    environment = var.environment
    project     = var.project_name
  }
}
