# 🔍 Databricks Cluster Optimization Analysis

A Databricks notebook for identifying cost optimization opportunities in your clusters by analyzing configurations against best practices.

> ⚠️ **Cloud Compatibility Note**:
> - **DBR Version Analysis** - ✅ Works on Azure, AWS, and GCP
> - **Driver Sizing Analysis** - ✅ Works on Azure, AWS, and GCP  
> - **VM Generation Analysis** - ⚠️ **Azure only** (uses Azure-specific VM naming conventions like `Standard_D4ds_v5`). AWS/GCP support may be added in future versions.

## 📋 Overview

This tool analyzes your Databricks clusters and identifies:

| Analysis Area | What We Check | Why It Matters |
|---------------|---------------|----------------|
| **DBR Versions** | Clusters on non-LTS or soon-to-expire LTS versions | Older runtimes miss performance improvements; out-of-support versions don't receive security patches |
| **VM Generations** | Clusters using older Azure VM generations (v3, v4) | Newer generations offer better price/performance |
| **Driver Sizing** | Oversized driver nodes (high vCPU/memory) | Drivers often don't need large VMs; right-sizing reduces costs |

## 🚀 Quick Start

1. Import the notebook into your Databricks workspace
2. Attach to a cluster with Unity Catalog access
3. Configure the widgets at the top
4. Run all cells
5. Review the Executive Summary

## 🔐 Prerequisites

### Required System Tables Access

This notebook queries Unity Catalog system tables:

- `system.compute.clusters` - Cluster configurations
- `system.compute.node_types` - Available node types with hardware info
- `system.billing.usage` - Usage/billing data
- `system.billing.list_prices` - List prices for cost calculation

### Granting Access to System Tables

**No user has access to system tables by default.** A **metastore admin** who is also an **account admin** must grant permissions:

```sql
-- Grant access to compute system tables
GRANT USE SCHEMA ON system.compute TO `user@example.com`;
GRANT SELECT ON system.compute.clusters TO `user@example.com`;
GRANT SELECT ON system.compute.node_types TO `user@example.com`;

-- Grant access to billing system tables
GRANT USE SCHEMA ON system.billing TO `user@example.com`;
GRANT SELECT ON system.billing.usage TO `user@example.com`;
GRANT SELECT ON system.billing.list_prices TO `user@example.com`;
```

Or grant to a group for easier management:

```sql
GRANT USE SCHEMA ON system.compute TO `data-analysts`;
GRANT SELECT ON system.compute.clusters TO `data-analysts`;
GRANT SELECT ON system.compute.node_types TO `data-analysts`;
GRANT USE SCHEMA ON system.billing TO `data-analysts`;
GRANT SELECT ON system.billing.usage TO `data-analysts`;
GRANT SELECT ON system.billing.list_prices TO `data-analysts`;
```

> **Note**: System tables are read-only and cannot be modified. See [Manage privileges in Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/index.html) for more details.

### Additional Permissions

- (Optional) Write access to the output catalog/schema for saving results

## ⚙️ Configuration

### Widgets

| Widget | Description | Default |
|--------|-------------|---------|
| **Lookback Period** | How far back to analyze (days) | 30 |
| **Workspace Filter** | Filter to specific workspace or ALL | ALL |
| **Driver CPU Threshold** | Max recommended driver vCPUs | 16 |
| **Driver Memory Threshold** | Max recommended driver memory (GB) | 64 |
| **Output Catalog** | Catalog for saving results | dbdemos_steventan |
| **Output Schema** | Schema for saving results | waf |

### VM Generation Thresholds (Azure Only)

Different Azure VM series have different recommended minimum generations:

| Series | Type | Default Threshold |
|--------|------|-------------------|
| D | General Purpose | v5 |
| E | Memory Optimized | v5 |
| F | Compute Optimized | v2 |
| L | Storage Optimized | v3 |
| M | Large Memory | v2 |
| N | GPU | v1 |

## 📊 Output

### Tables Generated

Each analysis section produces:
1. **Cluster-level detail** - Individual clusters with issues
2. **Distribution summary** - Aggregated view with cost impact

### Key Columns

| Column | Description |
|--------|-------------|
| `account_id` | Databricks account identifier |
| `workspace_id` | Workspace identifier |
| `cluster_id` | Unique cluster identifier |
| `total_dbus` | DBU consumption in lookback period |
| `total_cost_usd` | Dollar cost based on list prices |
| `support_status` / `vm_status` | Status indicator (🔴🟠🟢) |

### Status Legend

| Icon | Meaning |
|------|---------|
| 🔴 | **Critical** - Immediate action required |
| 🟠 | **Warning** - Plan action soon |
| 🟢 | **Good** - Meets best practices |
| ⛔ | **Expired** - Already past end-of-support |

## 📁 Files

```
├── Cluster_Optimization_Analysis.py    # Main notebook
└── README.md                           # This file
```

## 🔗 References

- [Compute System Tables Documentation](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/compute)
- [Cluster Policies Best Practices](https://docs.databricks.com/clusters/policy-best-practices.html)
- [Azure VM Pricing](https://azure.microsoft.com/pricing/details/virtual-machines/)
- [Databricks Runtime Release Notes](https://docs.databricks.com/release-notes/runtime/index.html)

## 📝 Version History

| Date | Author | Changes |
|------|--------|---------|
| 2025-12-10 | Steven Tan | Initial release |

## 👤 Author

**Steven Tan**  
Email: cheeyutcy@gmail.com

---

*Created for WAF Review*

