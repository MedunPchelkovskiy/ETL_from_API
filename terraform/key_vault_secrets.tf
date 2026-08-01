data "azurerm_key_vault_secret" "client_secret" {
  name         = "client-secret"
  key_vault_id = azurerm_key_vault.main.id
}

data "azurerm_key_vault_secret" "db_user" {
  name         = "db-user"
  key_vault_id = azurerm_key_vault.main.id
}

data "azurerm_key_vault_secret" "db_password" {
  name         = "db-password"
  key_vault_id = azurerm_key_vault.main.id
}

data "azurerm_key_vault_secret" "db_conn_raw" {
  name         = "db-conn-raw"
  key_vault_id = azurerm_key_vault.main.id
}

data "azurerm_key_vault_secret" "supabase_db_url" {
  name         = "supabase-db-url"
  key_vault_id = azurerm_key_vault.main.id
}

data "azurerm_key_vault_secret" "foreca_api_key" {
  name         = "foreca-api-key"
  key_vault_id = azurerm_key_vault.main.id
}

data "azurerm_key_vault_secret" "accuweather_api_key" {
  name         = "accuweather-api-key"
  key_vault_id = azurerm_key_vault.main.id
}

data "azurerm_key_vault_secret" "meteoblue_api_key" {
  name         = "meteoblue-api-key"
  key_vault_id = azurerm_key_vault.main.id
}

data "azurerm_key_vault_secret" "weatherbit_api_key" {
  name         = "weatherbit-api-key"
  key_vault_id = azurerm_key_vault.main.id
}

data "azurerm_key_vault_secret" "tommorow_api_key" {
  name         = "tommorow-api-key"
  key_vault_id = azurerm_key_vault.main.id
}

data "azurerm_key_vault_secret" "openweathermap_api_key" {
  name         = "openweathermap-api-key"
  key_vault_id = azurerm_key_vault.main.id
}

data "azurerm_key_vault_secret" "weatherapi_api_key" {
  name         = "weatherapi-api-key"
  key_vault_id = azurerm_key_vault.main.id
}

data "azurerm_key_vault_secret" "prefect_api_key" {
  name         = "prefect-api-key"
  key_vault_id = azurerm_key_vault.main.id
}