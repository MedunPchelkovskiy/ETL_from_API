output "resource_group_name" {
  value = azurerm_resource_group.main.name
}

output "container_app_environment_id" {
  value = azurerm_container_app_environment.main.id
}

# Internal DNS name other Container Apps in the same environment use to
# reach Pushgateway — this is what PUSHGATEWAY_URL should point to.
output "pushgateway_internal_fqdn" {
  value = azurerm_container_app.pushgateway.ingress[0].fqdn
}
