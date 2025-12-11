# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ Resource Utilization & Photon Analysis
# MAGIC 
# MAGIC This notebook analyzes cluster resource utilization to identify performance bottlenecks and optimization opportunities.
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 📋 What This Notebook Does
# MAGIC 
# MAGIC | Analysis Area | What We Check | Recommendation |
# MAGIC |---------------|---------------|----------------|
# MAGIC | **Photon Adoption** | Clusters not using Photon | Enable Photon for SQL/DataFrame workloads |
# MAGIC | **CPU Utilization** | High CPU usage on worker nodes | Enable Photon OR increase CPU cores |
# MAGIC | **I/O Wait** | High I/O wait percentage | Enable Delta Cache, optimize data layout |
# MAGIC | **Memory Pressure** | High memory usage / swapping | Use memory-optimized VMs (E-series), add workers |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 🎯 When Does Each Recommendation Apply?
# MAGIC 
# MAGIC | Bottleneck | Metric | Threshold | Action |
# MAGIC |------------|--------|-----------|--------|
# MAGIC | **CPU-bound** | `avg_cpu_percent` | >=50% | Enable Photon OR increase CPU cores |
# MAGIC | **I/O-bound** | `cpu_wait_percent` | >=10% | Enable Delta Cache, Z-ordering, partition pruning |
# MAGIC | **Memory-bound** | `memory_used_percent` | >=70% | Use larger nodes (E-series) or add workers |
# MAGIC | **Swapping** | `swap_used_percent` | >=5% | CRITICAL - Increase memory immediately |
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
# MAGIC > **Note**: The `system.compute.node_timeline` table is required for CPU, I/O, and memory analysis. If you don't have access, the notebook will fall back to cost-based recommendations.

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

# COMMAND ----------

# Get widget values
lookback_days = dbutils.widgets.get("lookback_days")
workspace_filter = dbutils.widgets.get("workspace_filter")
output_catalog = dbutils.widgets.get("output_catalog")
output_schema = dbutils.widgets.get("output_schema")

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

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5️⃣ Photon Adoption Analysis
# MAGIC 
# MAGIC **What is Photon?** Photon is Databricks' vectorized query engine that accelerates Spark SQL and DataFrame workloads.
# MAGIC 
# MAGIC **When does Photon help?**
# MAGIC - ✅ SQL/DataFrame heavy workloads (aggregations, joins, filters)
# MAGIC - ✅ ETL/ELT transformations
# MAGIC - ✅ CPU-bound operations
# MAGIC - ✅ **ML workloads using Spark SQL/DataFrames, feature engineering, GraphFrames, xgboost4j**
# MAGIC - ❌ Python UDFs, pandas operations
# MAGIC - ❌ Native Python ML (sklearn, PyTorch, TensorFlow)
# MAGIC 
# MAGIC **Trade-off**: Photon has a higher DBU rate but typically provides 2-8x performance improvement for compatible workloads.
# MAGIC 
# MAGIC > **Note**: Even ML runtimes can benefit from Photon if they use Spark SQL/DataFrames for data prep and feature engineering. See [Photon documentation](https://docs.databricks.com/aws/en/compute/photon#photon-features).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Photon Adoption Overview
# MAGIC 
# MAGIC This shows the overall Photon adoption rate across your clusters, with associated cost data.

# COMMAND ----------

# Photon Adoption Overview
photon_adoption_query = f"""
WITH cluster_costs AS (
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
    CASE 
        WHEN LOWER(c.dbr_version) LIKE '%photon%' THEN '⚡ Photon Enabled'
        ELSE '🔹 Standard (No Photon)'
    END AS photon_status,
    COUNT(DISTINCT c.cluster_id) AS cluster_count,
    ROUND(COUNT(DISTINCT c.cluster_id) * 100.0 / SUM(COUNT(DISTINCT c.cluster_id)) OVER (), 2) AS pct_of_clusters,
    ROUND(SUM(COALESCE(cc.total_dbus, 0)), 2) AS total_dbus,
    ROUND(SUM(COALESCE(cc.total_cost_usd, 0)), 2) AS total_cost_usd,
    ROUND(SUM(COALESCE(cc.total_cost_usd, 0)) * 100.0 / NULLIF(SUM(SUM(COALESCE(cc.total_cost_usd, 0))) OVER (), 0), 2) AS pct_of_total_cost
FROM system.compute.clusters c
LEFT JOIN cluster_costs cc ON c.cluster_id = cc.cluster_id
WHERE c.delete_time IS NULL
    AND c.change_time >= date_sub(current_date(), {lookback_days})
    AND {workspace_clause}
GROUP BY 
    CASE 
        WHEN LOWER(c.dbr_version) LIKE '%photon%' THEN '⚡ Photon Enabled'
        ELSE '🔹 Standard (No Photon)'
    END
ORDER BY cluster_count DESC
"""
display(spark.sql(photon_adoption_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6️⃣ Resource Utilization Analysis
# MAGIC 
# MAGIC This section uses `system.compute.node_timeline` to analyze CPU, I/O, and memory utilization patterns.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔥 High CPU Utilization Clusters (Best Photon Candidates)
# MAGIC 
# MAGIC Clusters with **high CPU utilization** are the best candidates for Photon because they are compute-bound.
# MAGIC 
# MAGIC **Logic**: 
# MAGIC - High CPU (>50% avg) + Not using Photon + Not ML Runtime = **Ideal Photon candidate**
# MAGIC - These workloads are spending time computing, not waiting on I/O
# MAGIC 
# MAGIC > **Note**: This query uses `system.compute.node_timeline` for utilization data. If you don't have access, this cell will show an error.

# COMMAND ----------

# High CPU Utilization Clusters - Best Photon Candidates
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
            WHEN LOWER(c.dbr_version) LIKE '%ml%' AND cpu.avg_cpu_percent >= 50 THEN '🟠 ML Runtime - Photon helps Spark SQL/feature eng; or use compute-optimized (F-series)'
            WHEN LOWER(c.dbr_version) LIKE '%ml%' THEN '🟡 ML Runtime - Evaluate if using Spark SQL/DataFrames'
            WHEN cpu.avg_cpu_percent >= 70 THEN '🔴 HIGH - Enable Photon, compute-optimized (F-series), larger nodes, or more workers'
            WHEN cpu.avg_cpu_percent >= 50 THEN '🟠 GOOD - Enable Photon, compute-optimized (F-series), larger nodes, or more workers'
            WHEN cpu.avg_cpu_percent >= 30 THEN '🟡 MODERATE - Consider Photon for SQL workloads'
            ELSE '🟢 LOW - Not CPU-bound (I/O, memory, or idle)'
        END AS recommendation
    FROM system.compute.clusters c
    JOIN cluster_cpu_stats cpu ON c.cluster_id = cpu.cluster_id
    LEFT JOIN cluster_costs cc ON c.cluster_id = cc.cluster_id
    WHERE c.delete_time IS NULL
        AND c.change_time >= date_sub(current_date(), {lookback_days})
        AND {workspace_clause}
        AND LOWER(c.dbr_version) NOT LIKE '%photon%'
        AND cpu.avg_cpu_percent >= 30  -- Only show clusters with meaningful CPU usage
    ORDER BY 
        cpu.avg_cpu_percent DESC,
        cc.total_cost_usd DESC NULLS LAST
    """
    display(spark.sql(high_cpu_photon_query))
except Exception as e:
    print(f"⚠️ Could not query CPU utilization data: {e}")
    print("This may be because system.compute.node_timeline is not available or you don't have access.")
    print("Falling back to cost-based Photon candidate analysis below.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 CPU Utilization Summary for Non-Photon Clusters
# MAGIC 
# MAGIC Aggregated view of CPU utilization patterns for clusters not using Photon.

# COMMAND ----------

# CPU Utilization Summary
try:
    cpu_summary_query = f"""
    WITH cluster_cpu_stats AS (
        SELECT 
            nt.cluster_id,
            AVG(nt.cpu_user_percent + nt.cpu_system_percent) AS avg_cpu_percent
        FROM system.compute.node_timeline nt
        WHERE nt.start_time >= date_sub(current_date(), {lookback_days})
            AND nt.driver = false  -- Only worker nodes
        GROUP BY nt.cluster_id
    ),
    cluster_costs AS (
        SELECT 
            u.usage_metadata.cluster_id,
            SUM(u.usage_quantity) AS total_dbus,
            SUM(u.usage_quantity * COALESCE(lp.pricing.default, 0)) AS total_cost_usd
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
        CASE 
            WHEN cpu.avg_cpu_percent >= 70 THEN '🔴 High CPU (>=70%)'
            WHEN cpu.avg_cpu_percent >= 50 THEN '🟠 Medium-High CPU (50-70%)'
            WHEN cpu.avg_cpu_percent >= 30 THEN '🟡 Medium CPU (30-50%)'
            ELSE '🟢 Low CPU (<30%)'
        END AS cpu_utilization_band,
        CASE 
            WHEN cpu.avg_cpu_percent >= 50 THEN '✅ Good Photon Candidate'
            WHEN cpu.avg_cpu_percent >= 30 THEN '⚠️ Evaluate for Photon'
            ELSE '❌ Not CPU-bound (I/O, memory, or other bottleneck)'
        END AS photon_suitability,
        COUNT(DISTINCT c.cluster_id) AS cluster_count,
        ROUND(SUM(COALESCE(cc.total_dbus, 0)), 2) AS total_dbus,
        ROUND(SUM(COALESCE(cc.total_cost_usd, 0)), 2) AS total_cost_usd
    FROM system.compute.clusters c
    JOIN cluster_cpu_stats cpu ON c.cluster_id = cpu.cluster_id
    LEFT JOIN cluster_costs cc ON c.cluster_id = cc.cluster_id
    WHERE c.delete_time IS NULL
        AND c.change_time >= date_sub(current_date(), {lookback_days})
        AND {workspace_clause}
        AND LOWER(c.dbr_version) NOT LIKE '%photon%'
    GROUP BY 
        CASE 
            WHEN cpu.avg_cpu_percent >= 70 THEN '🔴 High CPU (>=70%)'
            WHEN cpu.avg_cpu_percent >= 50 THEN '🟠 Medium-High CPU (50-70%)'
            WHEN cpu.avg_cpu_percent >= 30 THEN '🟡 Medium CPU (30-50%)'
            ELSE '🟢 Low CPU (<30%)'
        END,
        CASE 
            WHEN cpu.avg_cpu_percent >= 50 THEN '✅ Good Photon Candidate'
            WHEN cpu.avg_cpu_percent >= 30 THEN '⚠️ Evaluate for Photon'
            ELSE '❌ Likely I/O bound - Photon may not help'
        END
    ORDER BY 
        CASE 
            WHEN cpu.avg_cpu_percent >= 70 THEN 1
            WHEN cpu.avg_cpu_percent >= 50 THEN 2
            WHEN cpu.avg_cpu_percent >= 30 THEN 3
            ELSE 4
        END
    """
    display(spark.sql(cpu_summary_query))
except Exception as e:
    print(f"⚠️ Could not query CPU summary: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ### 💾 High I/O Wait Clusters (Delta Cache Candidates)
# MAGIC 
# MAGIC Clusters with **high I/O wait** are spending time waiting for storage. Recommendations:
# MAGIC - **Enable Delta Cache** - caches remote data on local SSD
# MAGIC - **Use Liquid Clustering** - modern replacement for Z-ordering (Z-ordering is deprecated)
# MAGIC - **Optimize data layout** - compaction, partition pruning
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
        HAVING AVG(nt.cpu_wait_percent) >= 10  -- Significant I/O wait
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
            WHEN io.avg_io_wait_percent >= 30 THEN '🔴 HIGH I/O WAIT - Enable Delta Cache + Liquid Clustering'
            WHEN io.avg_io_wait_percent >= 20 THEN '🟠 MEDIUM I/O WAIT - Consider Delta Cache, storage-optimized nodes'
            ELSE '🟡 MODERATE I/O WAIT - Review data layout, consider Liquid Clustering'
        END AS recommendation
    FROM system.compute.clusters c
    JOIN cluster_io_stats io ON c.cluster_id = io.cluster_id
    LEFT JOIN cluster_costs cc ON c.cluster_id = cc.cluster_id
    WHERE c.delete_time IS NULL
        AND c.change_time >= date_sub(current_date(), {lookback_days})
        AND {workspace_clause}
    ORDER BY io.avg_io_wait_percent DESC, cc.total_cost_usd DESC NULLS LAST
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
            ROUND(AVG(nt.memory_used_percent), 2) AS avg_memory_percent,
            ROUND(MAX(nt.memory_used_percent), 2) AS max_memory_percent,
            ROUND(AVG(nt.swap_used_percent), 2) AS avg_swap_percent,
            ROUND(MAX(nt.swap_used_percent), 2) AS max_swap_percent
        FROM system.compute.node_timeline nt
        WHERE nt.start_time >= date_sub(current_date(), {lookback_days})
            AND nt.driver = false
        GROUP BY nt.cluster_id
        HAVING AVG(nt.memory_used_percent) >= 70 OR AVG(nt.swap_used_percent) >= 5
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
            WHEN mem.avg_swap_percent >= 10 THEN '🔴 CRITICAL - Use memory-optimized (E-series), larger nodes, or more workers'
            WHEN mem.avg_swap_percent >= 5 THEN '🟠 SWAPPING - Use memory-optimized (E-series), larger nodes, or more workers'
            WHEN mem.avg_memory_percent >= 90 THEN '🔴 HIGH MEMORY - Use memory-optimized (E-series), larger nodes, or more workers'
            WHEN mem.avg_memory_percent >= 80 THEN '🟠 ELEVATED - Consider memory-optimized (E-series), larger nodes, or more workers'
            ELSE '🟡 MODERATE - Monitor for memory pressure'
        END AS recommendation
    FROM system.compute.clusters c
    JOIN cluster_memory_stats mem ON c.cluster_id = mem.cluster_id
    LEFT JOIN cluster_costs cc ON c.cluster_id = cc.cluster_id
    WHERE c.delete_time IS NULL
        AND c.change_time >= date_sub(current_date(), {lookback_days})
        AND {workspace_clause}
    ORDER BY mem.avg_swap_percent DESC, mem.avg_memory_percent DESC, cc.total_cost_usd DESC NULLS LAST
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
# MAGIC - **I/O-bound** → Enable Delta Cache, Liquid Clustering, storage-optimized (L-series)
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
            AVG(nt.memory_used_percent) AS avg_memory_percent,
            AVG(nt.swap_used_percent) AS avg_swap_percent
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
                WHEN cs.avg_swap_percent >= 5 OR cs.avg_memory_percent >= 80 THEN 'Memory-bound'
                WHEN cs.avg_io_wait_percent >= 20 THEN 'I/O-bound'
                WHEN cs.avg_cpu_percent >= 50 THEN 'CPU-bound'
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
            WHEN bottleneck_type = 'I/O-bound' THEN '💾 Enable Delta Cache, Liquid Clustering, storage-optimized (L-series)'
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
# MAGIC ### 🎯 Photon Candidates (Cost-Based Fallback)
# MAGIC 
# MAGIC If CPU utilization data is not available, these clusters are identified as Photon candidates based on:
# MAGIC - Not using ML runtime (Photon doesn't accelerate ML libraries)
# MAGIC - Not already using Photon
# MAGIC - Have significant DBU consumption (worth the upgrade effort)

# COMMAND ----------

# Photon Candidates - clusters that could benefit from Photon
photon_candidates_query = f"""
WITH cluster_costs AS (
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
    c.cluster_source,
    c.driver_node_type,
    c.worker_node_type,
    COALESCE(cc.total_dbus, 0) AS total_dbus,
    COALESCE(cc.total_cost_usd, 0) AS total_cost_usd,
    CASE 
        WHEN LOWER(c.dbr_version) LIKE '%ml%' AND COALESCE(cc.total_cost_usd, 0) > 500 THEN '🟠 ML Runtime - Photon helps Spark SQL/feature engineering'
        WHEN LOWER(c.dbr_version) LIKE '%ml%' THEN '🟡 ML Runtime - Evaluate if using Spark SQL/DataFrames'
        WHEN LOWER(c.dbr_version) LIKE '%gpu%' THEN '⚠️ GPU workloads - evaluate carefully'
        WHEN COALESCE(cc.total_cost_usd, 0) > 1000 THEN '🔴 High Priority - High spend cluster'
        WHEN COALESCE(cc.total_cost_usd, 0) > 100 THEN '🟠 Medium Priority'
        ELSE '🟢 Low Priority - Low spend'
    END AS photon_recommendation
FROM system.compute.clusters c
LEFT JOIN cluster_costs cc ON c.cluster_id = cc.cluster_id
WHERE c.delete_time IS NULL
    AND c.change_time >= date_sub(current_date(), {lookback_days})
    AND {workspace_clause}
    AND LOWER(c.dbr_version) NOT LIKE '%photon%'  -- Not already using Photon
ORDER BY 
    CASE 
        WHEN LOWER(c.dbr_version) LIKE '%ml%' THEN 3
        WHEN LOWER(c.dbr_version) LIKE '%gpu%' THEN 2
        ELSE 1
    END,
    cc.total_cost_usd DESC NULLS LAST
"""
display(spark.sql(photon_candidates_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📈 Photon Candidates Summary by Runtime Type
# MAGIC 
# MAGIC Summary of non-Photon clusters grouped by runtime type, showing potential impact.

# COMMAND ----------

# Photon Candidates Summary
photon_summary_query = f"""
WITH cluster_costs AS (
    SELECT 
        u.usage_metadata.cluster_id,
        SUM(u.usage_quantity) AS total_dbus,
        SUM(u.usage_quantity * COALESCE(lp.pricing.default, 0)) AS total_cost_usd
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
    CASE 
        WHEN LOWER(c.dbr_version) LIKE '%ml%' THEN '🤖 ML Runtime'
        WHEN LOWER(c.dbr_version) LIKE '%gpu%' THEN '🎮 GPU Runtime'
        ELSE '📊 Standard Runtime'
    END AS runtime_type,
    CASE 
        WHEN LOWER(c.dbr_version) LIKE '%ml%' THEN '🟡 Evaluate - Photon helps Spark SQL/feature eng, not sklearn/torch'
        WHEN LOWER(c.dbr_version) LIKE '%gpu%' THEN '⚠️ Evaluate - GPU workloads vary'
        ELSE '✅ Good Candidate for Photon'
    END AS photon_suitability,
    COUNT(DISTINCT c.cluster_id) AS cluster_count,
    ROUND(SUM(COALESCE(cc.total_dbus, 0)), 2) AS total_dbus,
    ROUND(SUM(COALESCE(cc.total_cost_usd, 0)), 2) AS total_cost_usd,
    ROUND(SUM(COALESCE(cc.total_cost_usd, 0)) * 100.0 / NULLIF(SUM(SUM(COALESCE(cc.total_cost_usd, 0))) OVER (), 0), 2) AS pct_of_non_photon_cost
FROM system.compute.clusters c
LEFT JOIN cluster_costs cc ON c.cluster_id = cc.cluster_id
WHERE c.delete_time IS NULL
    AND c.change_time >= date_sub(current_date(), {lookback_days})
    AND {workspace_clause}
    AND LOWER(c.dbr_version) NOT LIKE '%photon%'
GROUP BY 
    CASE 
        WHEN LOWER(c.dbr_version) LIKE '%ml%' THEN '🤖 ML Runtime'
        WHEN LOWER(c.dbr_version) LIKE '%gpu%' THEN '🎮 GPU Runtime'
        ELSE '📊 Standard Runtime'
    END,
    CASE 
        WHEN LOWER(c.dbr_version) LIKE '%ml%' THEN '⚠️ Evaluate - ML libraries not accelerated'
        WHEN LOWER(c.dbr_version) LIKE '%gpu%' THEN '⚠️ Evaluate - GPU workloads vary'
        ELSE '✅ Good Candidate for Photon'
    END
ORDER BY total_cost_usd DESC
"""
display(spark.sql(photon_summary_query))

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

