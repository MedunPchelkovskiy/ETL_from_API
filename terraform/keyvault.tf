# Needed to grant Key Vault access to "your own" tenant/current caller.
data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "main" {
  name                = "${var.project_name}-kv"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  # Soft-delete protects against accidental deletion of secrets; cheap
  # insurance, no extra cost.
  soft_delete_retention_days = 7
  purge_protection_enabled   = false # keep false for a portfolio project — true blocks vault deletion entirely
}

# One identity shared by all 5 Container Apps Jobs. They all need to read
# the same secrets (DB password, API keys), so one identity + one set of
# permissions is simpler than five separate ones.
resource "azurerm_user_assigned_identity" "etl_jobs" {
  name                = "${var.project_name}-jobs-identity"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
}

# Grants the jobs' identity permission to READ secrets — not to create or
# delete them. That's done separately, by you, via Azure CLI (see below).
resource "azurerm_key_vault_access_policy" "etl_jobs_read" {
  key_vault_id = azurerm_key_vault.main.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = azurerm_user_assigned_identity.etl_jobs.principal_id

  secret_permissions = ["Get", "List"]
}

# Grants YOUR current CLI login permission to manage secrets — so you can
# run `az keyvault secret set` from your machine. Without this, only the
# jobs' identity (read-only) would have access, and even you couldn't add
# secrets through the portal/CLI.
resource "azurerm_key_vault_access_policy" "admin_manage" {
  key_vault_id = azurerm_key_vault.main.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id

  secret_permissions = ["Get", "List", "Set", "Delete", "Purge"]
}
