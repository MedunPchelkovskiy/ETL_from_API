output "resource_group_name" {
  value = azurerm_resource_group.main.name
}


output "container_app_environment_id" {
  value = data.azurerm_container_app_environment.main.id
}


output "key_vault_name" {
  value = azurerm_key_vault.main.name
}


output "key_vault_uri" {
  value = azurerm_key_vault.main.vault_uri
}


output "pushgateway_internal_fqdn" {
  value = azurerm_container_app.pushgateway.latest_revision_fqdn
}


output "jobs_identity_id" {
  value = azurerm_user_assigned_identity.etl_jobs.id
}