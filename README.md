Raw JSON Data Ingestion on Azure (Portfolio Project)

Project Overview
This project demonstrates a modern data engineering workflow for ingesting raw JSON data using Azure storage concepts, while navigating real-world Azure subscription policies.

The architecture follows a Bronze–Silver–Gold (BSG) layered approach:

- Bronze: Raw, unprocessed JSON data (source ingestion)
- Silver: Cleansed, validated, and standardized data
- Gold: Analytics-ready aggregates for reporting or ML

Creating Azure Storage Account Script

This script demonstrates how to provision a resource group and a storage account in Azure using the Azure CLI. It is useful for projects that require cloud storage for ETL pipelines, data lakes, or general-purpose storage.
What it does:
    - Creates a resource group

        az group create --name <name_of_your_resource_group> --location <your_allowed_location>

        <name_of_your_resource_group> is the resource group name, <your_allowed_location> is the Azure region where all resources in this group will reside.

    - Creating a storage account

        az storage account create \
          --name <storage_account_name> \
          --resource-group <name_of_your_resource_group> \
          --location <your_allowed_location> \
          --sku Standard_LRS \
          --kind StorageV2


<storage_account_name> is the globally unique storage account name.
Standard_LRS provides standard locally-redundant storage.
StorageV2 (general-purpose v2) supports blobs, files, queues, and tables.
The storage account is deployed in the same region as the resource group to comply with Azure policies.



Note: Deployment of Azure Blob Storage / ADLS Gen2 is restricted under my Azure for Students subscription due to Allowed Resource Deployment Regions policies.
Deployment was blocked by subscription-level Azure Policies enforcing restricted deployment regions under Azure for Students, 
reflecting real-world governance constraints.
Implemented raw JSON ingestion into Microsoft Fabric OneLake, functionally equivalent to Azure Data Lake Storage Gen2.
This README documents the intended design and governance considerations.

---

    ┌─────────────┐
    │   Source    │
    │  JSON API   │
    └─────┬───────┘
          │
          ▼
    ┌─────────────┐
    │   Bronze    │
    │ Raw Storage │
    │  (ADLS Gen2│
    │  intended) │
    └─────┬───────┘
          │
          ▼
    ┌─────────────┐
    │   Silver    │
    │ Cleansed &  │
    │ Standardized│
    └─────┬───────┘
          │
          ▼
    ┌─────────────┐
    │    Gold     │
    │ Analytics & │
    │ ML-ready    │
    └─────────────┘


Designed a Bronze–Silver–Gold data architecture for raw JSON ingestion using Azure Blob Storage (ADLS Gen2) principles, while adhering to Azure governance and subscription policies.  
Implemented raw JSON ingestion into Microsoft Fabric OneLake, demonstrating a fully functional cloud-based ETL pipeline without deploying restricted storage accounts.  
Leveraged Azure CLI, hierarchical namespace, and standard LRS configuration to showcase enterprise-ready cloud storage skills.  
Documented subscription-level policy constraints and governance considerations, highlighting awareness of real-world cloud infrastructure limitations.  
Raw JSON data is streamed directly from the API into the Bronze layer of OneLake, bypassing local storage. 
This demonstrates an enterprise-ready ETL workflow, automating ingestion while maintaining security and scalability.
Uploaded API JSON directly into Microsoft Fabric OneLake using Azure SDK for Python and Azure AD authentication, without saving locally.
Used azure-storage-file-datalake SDK with DefaultAzureCredential to write files to a Lakehouse folder structure following a Bronze layer hierarchy.
