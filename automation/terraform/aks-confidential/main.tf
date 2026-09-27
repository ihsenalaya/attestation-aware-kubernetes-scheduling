##############################################################################
# Article 1 — Confidential AKS (node-level AMD SEV-SNP) on Standard DCasv6.
#
# REAL SEV-SNP — AKS DCasv6 WESTUS2. This provisions billable resources.
#
# Topology (<= 32 DCasv6 vCPU quota):
#   - system pool:       Standard_D2s_v3   (2 vCPU, non-confidential), 1 node
#   - confidential pool: Standard_DC8as_v6 (8 vCPU, AMD SEV-SNP Confidential VM)
#                        autoscale min=0 so it can be scaled to zero after runs.
#
# node-level SEV-SNP uses the DCasv6 Confidential VM family for this campaign.
# AKS provisions DCasv6 node pools as AMD SEV-SNP confidential VMs.
#
# Do NOT commit terraform.tfstate (see .gitignore in this directory).
##############################################################################

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.116"
    }
  }
}

provider "azurerm" {
  features {}
}

# westus2: DCasv6 (real AMD SEV-SNP) is exposed by ARM with quota here, unlike
# eastus2 where the granted DCASv5 SKUs are not exposed. See preflight evidence.
variable "region" {
  type    = string
  default = "westus2"
}

variable "resource_group_name" {
  type    = string
  default = "rg-article1-confidential"
}

variable "cluster_name" {
  type    = string
  default = "aks-article1"
}

variable "owner" {
  type    = string
  default = "article1"
}

variable "ttl" {
  type    = string
  default = "24h"
}

# Confidential VM SKU. westus2 exposes DCasv6 (DC4as_v6/DC8as_v6) as AMD
# SEV-SNP with quota available for the Article 1 rerun.
variable "confidential_vm_size" {
  type    = string
  default = "Standard_DC8as_v6"
}

variable "confidential_os_sku" {
  type    = string
  default = "Ubuntu"
}

variable "confidential_workload_runtime" {
  type    = string
  default = null
}

# Number of confidential nodes to bring up for the experiments.
# DC8as_v6 = 8 vCPU. 4 nodes = 32 vCPU (matches a 32-vCPU DCasv6 quota request).
variable "confidential_node_count" {
  type    = number
  default = 4
}

# Upper autoscale bound. Keep it equal to confidential_node_count so we never
# exceed the requested 32-vCPU confidential quota (4 x DC8as_v6 = 32).
variable "confidential_max_count" {
  type    = number
  default = 4
}

variable "system_vm_size" {
  type    = string
  default = "Standard_D2s_v3"
}

# Whether to create the confidential SEV-SNP node pool. Set to false when the
# confidential VM SKU is not exposed / has no quota in the region, so the cluster
# still deploys with only the (non-confidential) system pool. Real SEV-SNP
# experiments (gate G1) require this to be true with an available SKU.
variable "enable_confidential_pool" {
  type    = bool
  default = true
}

locals {
  tags = {
    project = "article1"
    owner   = var.owner
    ttl     = var.ttl
    env     = "research"
  }
}

resource "azurerm_resource_group" "this" {
  name     = var.resource_group_name
  location = var.region
  tags     = local.tags
}

resource "azurerm_kubernetes_cluster" "this" {
  name                = var.cluster_name
  location            = azurerm_resource_group.this.location
  resource_group_name = azurerm_resource_group.this.name
  dns_prefix          = var.cluster_name
  sku_tier            = "Free"
  oidc_issuer_enabled = true
  tags                = local.tags

  # Non-confidential system pool.
  default_node_pool {
    name       = "system"
    vm_size    = var.system_vm_size
    node_count = 1
    tags       = local.tags
  }

  identity {
    type = "SystemAssigned"
  }

  network_profile {
    network_plugin = "azure"
    network_policy = "azure"
  }
}

# Confidential VM node pool — AMD SEV-SNP at node level (DCasv6).
# DCasv6 provides confidential VM isolation for the node, but AKS rejects
# Kata pod-sandboxing on this SKU because it requires nested virtualization.
# Article 1 therefore treats DCasv6 results as real node-level SEV-SNP evidence.
# Created only when enable_confidential_pool = true (requires an exposed SKU with
# quota). When false, the cluster deploys system-only so it is still viewable.
resource "azurerm_kubernetes_cluster_node_pool" "confidential" {
  count                 = var.enable_confidential_pool ? 1 : 0
  name                  = "conf"
  kubernetes_cluster_id = azurerm_kubernetes_cluster.this.id
  vm_size               = var.confidential_vm_size
  os_sku                = var.confidential_os_sku
  workload_runtime      = var.confidential_workload_runtime

  # Scale-to-zero capable so the expensive pool is only up during experiments.
  # Default brings up the full DCasv6 quota (4 x DC8as_v6 = 32 vCPU) so the
  # scheduler has a real multi-node confidential pool to place across.
  enable_auto_scaling = true
  min_count           = 0
  max_count           = var.confidential_max_count
  node_count          = var.confidential_node_count

  node_labels = {
    "ai.sovereign.io/attested" = "pending"
    "ai.sovereign.io/tee"      = "SEV-SNP"
  }
  node_taints = [
    "ai.sovereign.io/confidential=true:NoSchedule"
  ]
  tags = local.tags
}

output "resource_group" {
  value = azurerm_resource_group.this.name
}

output "cluster_name" {
  value = azurerm_kubernetes_cluster.this.name
}

output "confidential_vm_size" {
  value = var.confidential_vm_size
}

output "confidential_pool_enabled" {
  value = var.enable_confidential_pool
}
