resource "azurerm_key_vault" "main" {

  name = "${var.project_name}-kv"

  location = azurerm_resource_group.main.location

  resource_group_name = azurerm_resource_group.main.name


  tenant_id = data.azurerm_client_config.current.tenant_id


  sku_name = "standard"


  soft_delete_retention_days = 7

  purge_protection_enabled = false
}



resource "azurerm_user_assigned_identity" "etl_jobs" {

  name = "${var.project_name}-jobs-identity"

  location = azurerm_resource_group.main.location

  resource_group_name = azurerm_resource_group.main.name
}



resource "azurerm_key_vault_access_policy" "admin_manage" {

  key_vault_id = azurerm_key_vault.main.id


  tenant_id = data.azurerm_client_config.current.tenant_id


  object_id = var.admin_object_id


  secret_permissions = [
    "Get",
    "List",
    "Set",
    "Delete",
    "Purge"
  ]
}



resource "azurerm_key_vault_access_policy" "etl_jobs_read" {

  key_vault_id = azurerm_key_vault.main.id


  tenant_id = data.azurerm_client_config.current.tenant_id

  object_id = azurerm_user_assigned_identity.etl_jobs.principal_id


  secret_permissions = [
    "Get",
    "List"
  ]
}