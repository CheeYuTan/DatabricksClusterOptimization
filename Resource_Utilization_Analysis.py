# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Resource Utilization Analysis
# MAGIC 
# MAGIC This notebook analyzes cluster resource utilization to identify performance bottlenecks and optimization opportunities.
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 📋 What This Notebook Does
# MAGIC 
# MAGIC | Bottleneck | What We Check | Recommendation |
# MAGIC |------------|---------------|----------------|
# MAGIC | **CPU-bound** | High CPU usage on worker nodes | Enable Photon, compute-optimized (F-series), larger nodes, more workers |
# MAGIC | **I/O-bound** | High I/O wait percentage | Enable Delta Cache, Liquid Clustering, storage-optimized (L-series) |
# MAGIC | **Memory-bound** | High memory or swap usage | Memory-optimized (E-series), larger nodes, more workers |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 🎯 Thresholds (Configurable via Widgets)
# MAGIC 
# MAGIC | Bottleneck | Metric | Default | Action |
# MAGIC |------------|--------|---------|--------|
# MAGIC | **CPU-bound** | `avg_cpu_percent` | >=70% | Enable Photon, compute-optimized (F-series), larger nodes, more workers |
# MAGIC | **I/O-bound** | `cpu_wait_percent` | >=10% | Enable Delta Cache, Liquid Clustering for effective file pruning, storage-optimized (L-series) |
# MAGIC | **Memory-bound** | `mem_used_percent` OR `mem_swap_percent` | >=80% OR >=1% | Memory-optimized (E-series), larger nodes, more workers |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 🔐 Prerequisites
# MAGIC 
# MAGIC This notebook requires access to **Unity Catalog System Tables**:
# MAGIC - `system.compute.clusters` - Cluster configurations
# MAGIC - `system.compute.node_timeline` - Resource utilization metrics (**required for detailed analysis**)
# MAGIC - `system.billing.usage` - Usage/billing data
# MAGIC - `system.billing.list_prices` - List prices for cost calculation
# MAGIC 
# MAGIC > **Note**: This notebook requires access to `system.compute.node_timeline` for CPU, I/O, and memory metrics.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ⚙️ Configuration Widgets

# COMMAND ----------

# Get available workspace IDs for the dropdown
try:
    workspace_ids_df = spark.sql("""
        SELECT DISTINCT workspace_id 
        FROM system.compute.clusters 
        WHERE workspace_id IS NOT NULL 
        ORDER BY workspace_id
    """)
    available_workspaces = [row.workspace_id for row in workspace_ids_df.collect()]
    workspace_options = ["ALL"] + available_workspaces
except:
    workspace_options = ["ALL"]

# Create widgets for configurable parameters
dbutils.widgets.dropdown("lookback_days", "30", ["7", "14", "30", "60", "90", "180"], "Lookback Period (Days)")
dbutils.widgets.combobox("workspace_filter", "ALL", workspace_options, "Workspace ID Filter")

# Output location for saving results
dbutils.widgets.text("output_catalog", "dbdemos_steventan", "Output Catalog")
dbutils.widgets.text("output_schema", "waf", "Output Schema")

# Resource utilization thresholds
dbutils.widgets.dropdown("cpu_threshold", "70", ["30", "50", "60", "70", "80", "90"], "CPU-bound Threshold (%)")
dbutils.widgets.dropdown("io_wait_threshold", "10", ["5", "10", "15", "20", "30"], "I/O Wait Threshold (%)")
dbutils.widgets.dropdown("memory_threshold", "80", ["60", "70", "80", "90"], "Memory Threshold (%)")
dbutils.widgets.dropdown("swap_threshold", "1", ["1", "2", "5", "10"], "Swap Threshold (%)")

# COMMAND ----------

# Get widget values
lookback_days = dbutils.widgets.get("lookback_days")
workspace_filter = dbutils.widgets.get("workspace_filter")
output_catalog = dbutils.widgets.get("output_catalog")
output_schema = dbutils.widgets.get("output_schema")

# Get threshold values
cpu_threshold = int(dbutils.widgets.get("cpu_threshold"))
io_wait_threshold = int(dbutils.widgets.get("io_wait_threshold"))
memory_threshold = int(dbutils.widgets.get("memory_threshold"))
swap_threshold = int(dbutils.widgets.get("swap_threshold"))

# Build workspace filter clause
if workspace_filter == "ALL":
    workspace_clause = "1=1"
else:
    workspace_clause = f"c.workspace_id = '{workspace_filter}'"

output_location = f"{output_catalog}.{output_schema}"

print(f"📊 Analysis Configuration:")
print(f"   • Lookback period: {lookback_days} days")
print(f"   • Workspace filter: {workspace_filter}")
print(f"   • Output location: {output_location}")
print(f"   • CPU-bound threshold: >= {cpu_threshold}%")
print(f"   • I/O wait threshold: >= {io_wait_threshold}%")
print(f"   • Memory-bound threshold: memory >= {memory_threshold}% OR swap >= {swap_threshold}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5️⃣ Resource Utilization Analysis
# MAGIC 
# MAGIC This section uses `system.compute.node_timeline` to analyze CPU, I/O, and memory utilization patterns.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔥 High CPU Utilization Clusters
# MAGIC 
# MAGIC Clusters with **high CPU utilization** are compute-bound and may benefit from:
# MAGIC - **Enable Photon** - 2-8x performance for SQL/DataFrame workloads
# MAGIC - **Compute-optimized nodes** - F-series VMs for higher clock speeds
# MAGIC - **Larger nodes or more workers** - More CPU cores to distribute load
# MAGIC 
# MAGIC > **Note**: This query uses `system.compute.node_timeline`. If you don't have access, this section will show an error.

# COMMAND ----------

# High CPU Utilization Clusters
# Using system.compute.node_timeline for CPU metrics
try:
    high_cpu_photon_query = f"""
    WITH cluster_cpu_stats AS (
        SELECT 
            nt.cluster_id,
            ROUND(AVG(nt.cpu_user_percent + nt.cpu_system_percent), 2) AS avg_cpu_percent,
            ROUND(MAX(nt.cpu_user_percent + nt.cpu_system_percent), 2) AS max_cpu_percent,
            COUNT(DISTINCT DATE(nt.start_time)) AS days_active
        FROM system.compute.node_timeline nt
        WHERE nt.start_time >= date_sub(current_date(), {lookback_days})
            AND nt.driver = false  -- Only worker nodes (where the actual compute happens)
        GROUP BY nt.cluster_id
    ),
    cluster_costs AS (
        SELECT 
            u.usage_metadata.cluster_id,
            ROUND(SUM(u.usage_quantity), 2) AS total_dbus,
            ROUND(SUM(u.usage_quantity * COALESCE(lp.pricing.default, 0)), 2) AS total_cost_usd
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices lp 
            ON u.sku_name = lp.sku_name
            AND u.usage_start_time >= lp.price_start_time
            AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
        WHERE u.usage_start_time >= date_sub(current_date(), {lookback_days})
            AND u.usage_metadata.cluster_id IS NOT NULL
        GROUP BY u.usage_metadata.cluster_id
    )
    SELECT 
        c.account_id,
        c.workspace_id,
        c.cluster_id,
        c.cluster_name,
        c.owned_by AS owner,
        c.dbr_version,
        CASE 
            WHEN LOWER(c.dbr_version) LIKE '%ml%' THEN '🤖 ML Runtime'
            WHEN LOWER(c.dbr_version) LIKE '%gpu%' THEN '🎮 GPU Runtime'
            ELSE '📊 Standard Runtime'
        END AS runtime_type,
        cpu.avg_cpu_percent,
        cpu.max_cpu_percent,
        cpu.days_active,
        COALESCE(cc.total_dbus, 0) AS total_dbus,
        COALESCE(cc.total_cost_usd, 0) AS total_cost_usd,
        CASE 
            WHEN LOWER(c.dbr_version) LIKE '%ml%' AND cpu.avg_cpu_percent >= {cpu_threshold} THEN '🟠 ML Runtime - Photon helps Spark SQL/feature eng; or F-series'
            WHEN LOWER(c.dbr_version) LIKE '%ml%' THEN '🟡 ML Runtime - Evaluate if using Spark SQL/DataFrames'
            WHEN cpu.avg_cpu_percent >= {cpu_threshold} THEN '🔴 CPU-bound - Photon, F-series, larger nodes, more workers'
            ELSE '🟢 Below threshold - Not CPU-bound'
        END AS recommendation
    FROM system.compute.clusters c
    JOIN cluster_cpu_stats cpu ON c.cluster_id = cpu.cluster_id
    LEFT JOIN cluster_costs cc ON c.cluster_id = cc.cluster_id
    WHERE c.delete_time IS NULL
        AND c.change_time >= date_sub(current_date(), {lookback_days})
        AND {workspace_clause}
        AND cpu.avg_cpu_percent >= {cpu_threshold}  -- Only show clusters above threshold
    ORDER BY cc.total_cost_usd DESC NULLS LAST
    """
    display(spark.sql(high_cpu_photon_query))
except Exception as e:
    print(f"⚠️ Could not query CPU utilization data: {e}")
    print("Ensure you have SELECT access to system.compute.node_timeline.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 💾 High I/O Wait Clusters
# MAGIC 
# MAGIC Clusters with **high I/O wait** are spending time waiting for storage. Recommendations:
# MAGIC - **Enable Delta Cache** - caches remote data on local SSD
# MAGIC - **Use Liquid Clustering** - effective file pruning (replaces Z-ordering)
# MAGIC - **Use storage-optimized nodes** - L-series VMs with local NVMe

# COMMAND ----------

# High I/O Wait Clusters - Delta Cache Candidates
try:
    high_io_wait_query = f"""
    WITH cluster_io_stats AS (
        SELECT 
            nt.cluster_id,
            ROUND(AVG(nt.cpu_wait_percent), 2) AS avg_io_wait_percent,
            ROUND(MAX(nt.cpu_wait_percent), 2) AS max_io_wait_percent,
            ROUND(AVG(nt.cpu_user_percent + nt.cpu_system_percent), 2) AS avg_cpu_percent
        FROM system.compute.node_timeline nt
        WHERE nt.start_time >= date_sub(current_date(), {lookback_days})
            AND nt.driver = false
        GROUP BY nt.cluster_id
        HAVING AVG(nt.cpu_wait_percent) >= {io_wait_threshold}  -- Significant I/O wait
    ),
    cluster_costs AS (
        SELECT 
            u.usage_metadata.cluster_id,
            ROUND(SUM(u.usage_quantity), 2) AS total_dbus,
            ROUND(SUM(u.usage_quantity * COALESCE(lp.pricing.default, 0)), 2) AS total_cost_usd
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices lp 
            ON u.sku_name = lp.sku_name
            AND u.usage_start_time >= lp.price_start_time
            AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
        WHERE u.usage_start_time >= date_sub(current_date(), {lookback_days})
            AND u.usage_metadata.cluster_id IS NOT NULL
        GROUP BY u.usage_metadata.cluster_id
    )
    SELECT 
        c.account_id,
        c.workspace_id,
        c.cluster_id,
        c.cluster_name,
        c.owned_by AS owner,
        c.dbr_version,
        io.avg_io_wait_percent,
        io.max_io_wait_percent,
        io.avg_cpu_percent,
        c.driver_node_type,
        c.worker_node_type,
        COALESCE(cc.total_dbus, 0) AS total_dbus,
        COALESCE(cc.total_cost_usd, 0) AS total_cost_usd,
        CASE 
            WHEN io.avg_io_wait_percent >= {io_wait_threshold} * 2 THEN '🔴 HIGH - Delta Cache + Liquid Clustering for file pruning'
            WHEN io.avg_io_wait_percent >= {io_wait_threshold} THEN '🟠 I/O-bound - Delta Cache, Liquid Clustering, L-series'
            ELSE '🟡 MODERATE - Consider Liquid Clustering for file pruning'
        END AS recommendation
    FROM system.compute.clusters c
    JOIN cluster_io_stats io ON c.cluster_id = io.cluster_id
    LEFT JOIN cluster_costs cc ON c.cluster_id = cc.cluster_id
    WHERE c.delete_time IS NULL
        AND c.change_time >= date_sub(current_date(), {lookback_days})
        AND {workspace_clause}
    ORDER BY cc.total_cost_usd DESC NULLS LAST
    """
    display(spark.sql(high_io_wait_query))
except Exception as e:
    print(f"⚠️ Could not query I/O wait data: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 🧠 High Memory Utilization Clusters
# MAGIC 
# MAGIC Clusters with **high memory usage or swap activity** are memory-constrained. Recommendations:
# MAGIC - **Use memory-optimized nodes** - E-series VMs
# MAGIC - **Use larger nodes** - More memory per node
# MAGIC - **Add more workers** - Distribute memory pressure across cluster
# MAGIC - **Optimize queries** - Reduce shuffles, use broadcast joins for small tables

# COMMAND ----------

# High Memory Utilization Clusters
try:
    high_memory_query = f"""
    WITH cluster_memory_stats AS (
        SELECT 
            nt.cluster_id,
            ROUND(AVG(nt.mem_used_percent), 2) AS avg_memory_percent,
            ROUND(MAX(nt.mem_used_percent), 2) AS max_memory_percent,
            ROUND(AVG(nt.mem_swap_percent), 2) AS avg_swap_percent,
            ROUND(MAX(nt.mem_swap_percent), 2) AS max_swap_percent
        FROM system.compute.node_timeline nt
        WHERE nt.start_time >= date_sub(current_date(), {lookback_days})
            AND nt.driver = false
        GROUP BY nt.cluster_id
        HAVING AVG(nt.mem_used_percent) >= {memory_threshold} OR AVG(nt.mem_swap_percent) >= {swap_threshold}
    ),
    cluster_costs AS (
        SELECT 
            u.usage_metadata.cluster_id,
            ROUND(SUM(u.usage_quantity), 2) AS total_dbus,
            ROUND(SUM(u.usage_quantity * COALESCE(lp.pricing.default, 0)), 2) AS total_cost_usd
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices lp 
            ON u.sku_name = lp.sku_name
            AND u.usage_start_time >= lp.price_start_time
            AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
        WHERE u.usage_start_time >= date_sub(current_date(), {lookback_days})
            AND u.usage_metadata.cluster_id IS NOT NULL
        GROUP BY u.usage_metadata.cluster_id
    )
    SELECT 
        c.account_id,
        c.workspace_id,
        c.cluster_id,
        c.cluster_name,
        c.owned_by AS owner,
        c.dbr_version,
        mem.avg_memory_percent,
        mem.max_memory_percent,
        mem.avg_swap_percent,
        mem.max_swap_percent,
        c.driver_node_type,
        c.worker_node_type,
        COALESCE(cc.total_dbus, 0) AS total_dbus,
        COALESCE(cc.total_cost_usd, 0) AS total_cost_usd,
        CASE 
            WHEN mem.avg_swap_percent >= {swap_threshold} THEN '🔴 SWAPPING - E-series, larger nodes, more workers'
            WHEN mem.avg_memory_percent >= {memory_threshold} THEN '🟠 Memory-bound - E-series, larger nodes, more workers'
            ELSE '🟡 MODERATE - Monitor for memory pressure'
        END AS recommendation
    FROM system.compute.clusters c
    JOIN cluster_memory_stats mem ON c.cluster_id = mem.cluster_id
    LEFT JOIN cluster_costs cc ON c.cluster_id = cc.cluster_id
    WHERE c.delete_time IS NULL
        AND c.change_time >= date_sub(current_date(), {lookback_days})
        AND {workspace_clause}
    ORDER BY cc.total_cost_usd DESC NULLS LAST
    """
    display(spark.sql(high_memory_query))
except Exception as e:
    print(f"⚠️ Could not query memory utilization data: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 📊 Resource Utilization Summary
# MAGIC 
# MAGIC Overview of resource bottlenecks across all clusters:
# MAGIC - **CPU-bound** → Enable Photon, compute-optimized (F-series), larger nodes, or more workers
# MAGIC - **I/O-bound** → Delta Cache, Liquid Clustering for file pruning, storage-optimized (L-series)
# MAGIC - **Memory-bound** → Memory-optimized (E-series), larger nodes, or more workers

# COMMAND ----------

# Resource Utilization Summary
try:
    resource_summary_query = f"""
    WITH cluster_stats AS (
        SELECT 
            nt.cluster_id,
            AVG(nt.cpu_user_percent + nt.cpu_system_percent) AS avg_cpu_percent,
            AVG(nt.cpu_wait_percent) AS avg_io_wait_percent,
            AVG(nt.mem_used_percent) AS avg_memory_percent,
            AVG(nt.mem_swap_percent) AS avg_swap_percent
        FROM system.compute.node_timeline nt
        WHERE nt.start_time >= date_sub(current_date(), {lookback_days})
            AND nt.driver = false
        GROUP BY nt.cluster_id
    ),
    cluster_costs AS (
        SELECT 
            u.usage_metadata.cluster_id,
            SUM(u.usage_quantity * COALESCE(lp.pricing.default, 0)) AS total_cost_usd
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices lp 
            ON u.sku_name = lp.sku_name
            AND u.usage_start_time >= lp.price_start_time
            AND (lp.price_end_time IS NULL OR u.usage_start_time < lp.price_end_time)
        WHERE u.usage_start_time >= date_sub(current_date(), {lookback_days})
            AND u.usage_metadata.cluster_id IS NOT NULL
        GROUP BY u.usage_metadata.cluster_id
    ),
    categorized AS (
        SELECT 
            c.cluster_id,
            cs.avg_cpu_percent,
            cs.avg_io_wait_percent,
            cs.avg_memory_percent,
            cs.avg_swap_percent,
            cc.total_cost_usd,
            CASE 
                WHEN cs.avg_swap_percent >= {swap_threshold} OR cs.avg_memory_percent >= {memory_threshold} THEN 'Memory-bound'
                WHEN cs.avg_io_wait_percent >= {io_wait_threshold} THEN 'I/O-bound'
                WHEN cs.avg_cpu_percent >= {cpu_threshold} THEN 'CPU-bound'
                ELSE 'Balanced/Underutilized'
            END AS bottleneck_type
        FROM system.compute.clusters c
        JOIN cluster_stats cs ON c.cluster_id = cs.cluster_id
        LEFT JOIN cluster_costs cc ON c.cluster_id = cc.cluster_id
        WHERE c.delete_time IS NULL
            AND c.change_time >= date_sub(current_date(), {lookback_days})
            AND {workspace_clause}
    )
    SELECT 
        bottleneck_type,
        CASE 
            WHEN bottleneck_type = 'CPU-bound' THEN '⚡ Enable Photon, compute-optimized (F-series), larger nodes, or more workers'
            WHEN bottleneck_type = 'I/O-bound' THEN '💾 Delta Cache, Liquid Clustering for file pruning, storage-optimized (L-series)'
            WHEN bottleneck_type = 'Memory-bound' THEN '🧠 Memory-optimized (E-series), larger nodes, or more workers'
            ELSE '✅ No immediate action needed'
        END AS recommendation,
        COUNT(*) AS cluster_count,
        ROUND(SUM(COALESCE(total_cost_usd, 0)), 2) AS total_cost_usd,
        ROUND(SUM(COALESCE(total_cost_usd, 0)) * 100.0 / NULLIF(SUM(SUM(COALESCE(total_cost_usd, 0))) OVER (), 0), 2) AS pct_of_total_cost
    FROM categorized
    GROUP BY bottleneck_type
    ORDER BY total_cost_usd DESC
    """
    display(spark.sql(resource_summary_query))
except Exception as e:
    print(f"⚠️ Could not query resource summary: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🔗 Quick Reference Links
# MAGIC 
# MAGIC - [Compute System Tables Documentation](https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/compute)
# MAGIC - [Photon Runtime](https://docs.databricks.com/runtime/photon.html)
# MAGIC - [Delta Cache](https://docs.databricks.com/delta/optimizations/delta-cache.html)
# MAGIC - [Node Timeline Table](https://docs.databricks.com/administration-guide/system-tables/compute.html#node-timeline)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Created for WAF Review**  
# MAGIC **Last Updated**: 10 Dec 2025  
# MAGIC **Author**: Steven Tan (cheeyutcy@gmail.com)

