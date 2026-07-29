terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
  }

  # State remotely (Azure Storage) preferred over local state once you have
  # more than one person/machine touching this. Local state is fine solo.
  # backend "azurerm" {
  #   resource_group_name  = "tfstate-rg"
  #   storage_account_name = "yourtfstatestorage"
  #   container_name       = "tfstate"
  #   key                  = "weather-etl.tfstate"
  # }
}

provider "azurerm" {
  features {}
}
