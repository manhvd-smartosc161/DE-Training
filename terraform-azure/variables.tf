variable "resource_group_name" {
  description = "Name of the resource group"
  type        = string
  default     = "manhvd-resources"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "Southeast Asia"
}

variable "vm_size" {
  description = "Size of the VM"
  type        = string
  default     = "Standard_B1s"
}

variable "admin_username" {
  description = "Username for the VM"
  type        = string
  default     = "azureuser"
}

variable "ssh_public_key" {
  description = "Path to the public SSH key"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
} 