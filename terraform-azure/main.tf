# Configure the Azure provider
# This block sets up the Azure provider with default features
provider "azurerm" {
  features {}
  subscription_id = "571c5cf5-5725-415d-9dff-fb88e8d569a5"
  tenant_id       = "d4e484ed-0018-471f-b7c2-3265f48203f3"  # Replace with your actual tenant ID (not a placeholder)
}

# Create a resource group
# Resource groups are logical containers for Azure resources
resource "azurerm_resource_group" "rg" {
  name     = "manhvd-resources"  # Name of the resource group
  location = "Southeast Asia"    # Geographic location for the resource group
}

# Create virtual network
# Virtual networks provide isolated networking for Azure resources
resource "azurerm_virtual_network" "vnet" {
  name                = "manhvd-network"       # Name of the virtual network
  address_space       = ["10.0.0.0/16"]        # IP address space for the network
  location            = azurerm_resource_group.rg.location      # Use the same location as the resource group
  resource_group_name = azurerm_resource_group.rg.name          # Associate with the resource group
}

# Create subnet
# Subnets divide the virtual network into smaller address spaces
resource "azurerm_subnet" "subnet" {
  name                 = "manhvd-subnet"                      # Name of the subnet
  resource_group_name  = azurerm_resource_group.rg.name       # Associate with the resource group
  virtual_network_name = azurerm_virtual_network.vnet.name    # Associate with the virtual network
  address_prefixes     = ["10.0.1.0/24"]                      # IP address range for the subnet
}

# Create public IP
# Public IPs allow resources to be accessible from the internet
resource "azurerm_public_ip" "public_ip" {
  name                = "manhvd-public-ip"                   # Name of the public IP resource
  location            = azurerm_resource_group.rg.location   # Use the same location as the resource group
  resource_group_name = azurerm_resource_group.rg.name       # Associate with the resource group
  allocation_method   = "Static"
  sku                 = "Standard"
  zones               = ["1"]
}

# Create Network Security Group
# NSGs control inbound and outbound traffic to network interfaces
resource "azurerm_network_security_group" "nsg" {
  name                = "manhvd-nsg"                         # Name of the NSG
  location            = azurerm_resource_group.rg.location   # Use the same location as the resource group
  resource_group_name = azurerm_resource_group.rg.name       # Associate with the resource group

  # Define a security rule to allow SSH access
  security_rule {
    name                       = "SSH"                       # Name of the security rule
    priority                   = 1001                        # Priority of the rule (lower numbers are processed first)
    direction                  = "Inbound"                   # Rule applies to incoming traffic
    access                     = "Allow"                     # Allow the traffic that matches this rule
    protocol                   = "Tcp"                       # Apply to TCP protocol
    source_port_range          = "*"                         # Apply to all source ports
    destination_port_range     = "22"                        # Apply to SSH port (22)
    source_address_prefix      = "*"                         # Apply to all source IP addresses
    destination_address_prefix = "*"                         # Apply to all destination IP addresses
  }
}

# Create network interface
# Network interfaces connect VMs to a virtual network
resource "azurerm_network_interface" "nic" {
  name                = "manhvd-nic"                         # Name of the network interface
  location            = azurerm_resource_group.rg.location   # Use the same location as the resource group
  resource_group_name = azurerm_resource_group.rg.name       # Associate with the resource group

  # Configure the IP settings for the network interface
  ip_configuration {
    name                          = "internal"                           # Name of the IP configuration
    subnet_id                     = azurerm_subnet.subnet.id             # Associate with the subnet
    private_ip_address_allocation = "Dynamic"                            # Allocate private IP dynamically
    public_ip_address_id          = azurerm_public_ip.public_ip.id       # Associate with the public IP
  }
}

# Connect the security group to the network interface
# This associates the NSG with the network interface to apply security rules
resource "azurerm_network_interface_security_group_association" "nsg_association" {
  network_interface_id      = azurerm_network_interface.nic.id           # Reference to the network interface
  network_security_group_id = azurerm_network_security_group.nsg.id      # Reference to the NSG
}

# Create SSH key
# Generate an RSA key pair for secure SSH access
resource "tls_private_key" "ssh" {
  algorithm = "RSA"                                                      # Use RSA algorithm for key generation
  rsa_bits  = 4096                                                       # Use 4096 bits for strong security
}

# Save SSH key to file
# Store the private key locally for SSH access
resource "local_file" "ssh_key" {
  filename        = "azure_vm_key.pem"                                   # File name for the private key
  content         = tls_private_key.ssh.private_key_pem                  # Content is the generated private key
  file_permission = "0600"                                               # Set secure file permissions (owner read/write only)
}

# Create virtual machine
# Deploy a Linux VM in Azure
resource "azurerm_linux_virtual_machine" "vm" {
  name                = "manhvd-vm"                                      # Name of the VM
  resource_group_name = azurerm_resource_group.rg.name                   # Associate with the resource group
  location            = azurerm_resource_group.rg.location               # Use the same location as the resource group
  size                = "Standard_B1ls"                                  # VM size (CPU, memory) - Free tier eligible
  admin_username      = "azureuser"                                      # Username for SSH access

  # Connect the VM to the network interface
  network_interface_ids = [
    azurerm_network_interface.nic.id,                                    # Reference to the network interface
  ]

  # Configure SSH access using the generated key
  admin_ssh_key {
    username   = "azureuser"                                             # Username for SSH access
    public_key = tls_private_key.ssh.public_key_openssh                  # Public key for SSH authentication
  }

  # Configure the OS disk for the VM
  os_disk {
    caching              = "ReadWrite"                                   # Enable read/write caching for better performance
    storage_account_type = "Standard_LRS"                                # Use standard locally redundant storage
    disk_size_gb         = 30                                            # Minimum disk size for free tier
  }

  # Specify the OS image to use
  source_image_reference {
    publisher = "Canonical"                                              # Image publisher (Canonical for Ubuntu)
    offer     = "UbuntuServer"                                           # Image offer (Ubuntu Server)
    sku       = "18.04-LTS"                                              # Image SKU (Ubuntu 18.04 LTS)
    version   = "latest"                                                 # Use the latest version of the image
  }

  # Add tags to identify this as a free tier VM
  tags = {
    environment = "development"
    tier        = "free"
  }
}

# Create Azure Storage Account (equivalent to AWS S3)
# Storage Accounts are the foundation for Azure storage services
resource "azurerm_storage_account" "storage" {
  name                     = "manhvdstorage"                            # Globally unique storage account name (lowercase letters and numbers only)
  resource_group_name      = azurerm_resource_group.rg.name             # Associate with the resource group
  location                 = azurerm_resource_group.rg.location         # Use the same location as the resource group
  account_tier             = "Standard"                                 # Performance tier (Standard or Premium)
  account_replication_type = "LRS"                                      # Locally redundant storage (similar to S3 standard)
  account_kind             = "StorageV2"                                # General-purpose v2 storage account
  is_hns_enabled          = true                                       # Enable hierarchical namespace for Data Lake Gen2
  
  # Enable blob service properties
  blob_properties {
    # Configure data protection with soft delete for blobs
    delete_retention_policy {
      days = 7                                                          # Retain deleted blobs for 7 days
    }
    
    # Configure container delete retention policy
    container_delete_retention_policy {
      days = 7                                                          # Retain deleted containers for 7 days
    }
  }
  
  # Add network rules to restrict access (similar to S3 bucket policies)
  network_rules {
    default_action = "Allow"                                            # Allow access by default
    ip_rules       = []                                                 # Can be restricted to specific IPs
    bypass         = ["AzureServices"]                                  # Allow Azure services to access the storage
  }
  
  # Add tags for identification
  tags = {
    environment = "development"
    purpose     = "file-storage"
  }
}

# Create Data Lake Gen2 Filesystem
resource "azurerm_storage_data_lake_gen2_filesystem" "datalake" {
  name               = "manhvd-datalake"
  storage_account_id = azurerm_storage_account.storage.id

  # Cấu hình quyền truy cập
  ace {
    type        = "user"
    permissions = "rwx"
  }
  ace {
    type        = "group"
    permissions = "r-x"
  }
  ace {
    type        = "other"
    permissions = "r-x"
  }
}

# Create a container within the storage account (equivalent to an S3 bucket)
resource "azurerm_storage_container" "container" {
  name                  = "manhvd-container"                            # Name of the container
  storage_account_name  = azurerm_storage_account.storage.name          # Reference to the storage account
  container_access_type = "private"                                     # Private access (requires authentication)
}

# Configure lifecycle management for the storage account (similar to S3 lifecycle rules)
resource "azurerm_storage_management_policy" "lifecycle" {
  storage_account_id = azurerm_storage_account.storage.id               # Reference to the storage account

  rule {
    name    = "cleanup-old-versions"                                    # Name of the lifecycle rule
    enabled = true                                                      # Enable the rule
    
    filters {
      prefix_match = ["documents/"]                                     # Apply to objects with this prefix
      blob_types   = ["blockBlob"]                                      # Apply to block blobs
    }
    
    actions {
      version {
        delete_after_days_since_creation = 30                           # Delete old versions after 30 days
      }
      
      base_blob {
        tier_to_cool_after_days_since_modification_greater_than = 90    # Move to cool storage after 90 days
        tier_to_archive_after_days_since_modification_greater_than = 180 # Move to archive storage after 180 days
        delete_after_days_since_modification_greater_than = 365         # Delete after 1 year
      }
    }
  }
}

# Output the public IP address
# Display the public IP after deployment for easy access
output "public_ip_address" {
  value = azurerm_public_ip.public_ip.ip_address                         # Reference to the public IP address
}

# Output the storage account details
output "storage_account_name" {
  value = azurerm_storage_account.storage.name                           # Output the storage account name
}

output "storage_account_primary_access_key" {
  value     = azurerm_storage_account.storage.primary_access_key         # Output the primary access key
  sensitive = true                                                       # Mark as sensitive to hide in logs
}

output "storage_container_name" {
  value = azurerm_storage_container.container.name                       # Output the container name
}

# Create Azure Synapse Analytics Workspace (similar to BigQuery)
resource "azurerm_synapse_workspace" "synapse" {
  name                                 = "manhvd-synapse"
  resource_group_name                  = azurerm_resource_group.rg.name
  location                            = azurerm_resource_group.rg.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.datalake.id
  sql_administrator_login              = "sqladmin"
  sql_administrator_login_password     = "H@Sh1CoR3!"  # Thay đổi mật khẩu này trong môi trường production

  # Cấu hình managed virtual network
  managed_virtual_network_enabled = true

  # Cấu hình identity
  identity {
    type = "SystemAssigned"
  }

  # Cấu hình SQL
  sql_identity_control_enabled = true

  tags = {
    environment = "development"
    purpose     = "data-warehouse"
  }
}

# Create Synapse Spark Pool (for big data processing)
resource "azurerm_synapse_spark_pool" "spark_pool" {
  name                 = "manhvdspark"  # Tên ngắn hơn (1-15 ký tự)
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  node_size_family     = "MemoryOptimized"
  node_size            = "Small"
  spark_version        = "3.3"

  # Cấu hình số lượng nodes tối thiểu
  auto_scale {
    max_node_count = 3  # Giữ nguyên 3 nodes
    min_node_count = 3  # Phải có ít nhất 3 nodes
  }

  # Tự động tắt nhanh hơn khi không sử dụng
  auto_pause {
    delay_in_minutes = 5  # Tắt sau 5 phút không sử dụng
  }

  # Cấu hình thư viện
  library_requirement {
    content  = <<EOF
    numpy==1.21.0
    pandas==1.3.0
    EOF
    filename = "requirements.txt"
  }
}

# Tạo linked service để kết nối với storage
resource "azurerm_synapse_linked_service" "storage_linked_service" {
  name                 = "manhvd-storage-linked-service"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  type                 = "AzureBlobStorage"
  type_properties_json = jsonencode({
    connectionString = azurerm_storage_account.storage.primary_connection_string
  })
}

# Add outputs for Synapse
output "synapse_workspace_name" {
  value = azurerm_synapse_workspace.synapse.name
}

output "synapse_spark_pool_name" {
  value = azurerm_synapse_spark_pool.spark_pool.name
}

output "synapse_workspace_endpoint" {
  value = azurerm_synapse_workspace.synapse.connectivity_endpoints
}