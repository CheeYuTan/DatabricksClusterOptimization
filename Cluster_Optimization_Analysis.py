# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Databricks Cluster Optimization Analysis
# MAGIC 
# MAGIC This notebook helps you identify **cost optimization opportunities** in your Databricks clusters by analyzing configurations against best practices.
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 📋 What This Notebook Does
# MAGIC 
# MAGIC | Analysis Area | What We Check | Why It Matters |
# MAGIC |---------------|---------------|----------------|
# MAGIC | **DBR Versions** | Clusters on non-LTS or soon-to-expire LTS versions | Older runtimes miss performance improvements; out-of-support versions don't receive security patches |
# MAGIC | **VM Generations** | Clusters using older Azure VM generations (v3, v4) | Newer generations offer better price/performance |
# MAGIC | **Driver Sizing** | Oversized driver nodes (high vCPU/memory) | Drivers often don't need large VMs; right-sizing reduces costs |
# MAGIC | **Photon Adoption** | Clusters not using Photon runtime | Photon provides 2-8x performance for SQL/DataFrame workloads |
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 🎯 How to Use This Notebook
# MAGIC 
# MAGIC 1. **Configure the widgets** at the top to set your analysis parameters
# MAGIC 2. **Run all cells** to generate the analysis
# MAGIC 3. **Review the tables** - sorted by urgency and cost impact
# MAGIC 4. **Check the Executive Summary** for a quick overview of findings
# MAGIC 5. **Export or share** results with your team
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 🔐 Prerequisites
# MAGIC 
# MAGIC This notebook requires access to **Unity Catalog System Tables**:
# MAGIC - `system.compute.clusters` - Cluster configurations
# MAGIC - `system.compute.node_types` - Available node types with hardware info
# MAGIC - `system.billing.usage` - Usage/billing data
# MAGIC - `system.billing.list_prices` - List prices for cost calculation
# MAGIC 
# MAGIC > **Note**: You need appropriate permissions to query these system tables. Contact your workspace admin if you encounter access errors.
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## 📊 Status Legend
# MAGIC 
# MAGIC | Icon | Meaning |
# MAGIC |------|---------|
# MAGIC | 🔴 | **Critical** - Immediate action required |
# MAGIC | 🟠 | **Warning** - Plan action soon |
# MAGIC | 🟢 | **Good** - Meets best practices |
# MAGIC | ⛔ | **Expired** - Already past end-of-support |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## ⚙️ Configuration Widgets
# MAGIC 
# MAGIC Use the widgets below to customize your analysis. After changing values, re-run the notebook to see updated results.
# MAGIC 
# MAGIC | Widget | Description |
# MAGIC |--------|-------------|
# MAGIC | **Lookback Period** | How far back to analyze cluster activity (7-180 days) |
# MAGIC | **Workspace Filter** | Filter to a specific workspace or analyze all |
# MAGIC | **Driver CPU/Memory Threshold** | Flag drivers exceeding these specs as "oversized" |
# MAGIC | **VM Generation Thresholds** | Minimum acceptable VM generation per series (D, E, F, L, M, N) |
# MAGIC | **Output Catalog/Schema** | Location to save analysis results (for historical tracking) |

# COMMAND ----------

# First, get available workspace IDs for the dropdown
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
dbutils.widgets.text("latest_lts_version", "17.3", "Latest LTS DBR Version")
dbutils.widgets.dropdown("driver_cpu_threshold", "16", ["8", "16", "32", "48", "64"], "Max Recommended Driver vCPUs")
dbutils.widgets.dropdown("driver_memory_gb_threshold", "64", ["32", "64", "128", "256"], "Max Recommended Driver Memory (GB)")
dbutils.widgets.combobox("workspace_filter", "ALL", workspace_options, "Workspace ID Filter")

# Output location for saving results
dbutils.widgets.text("output_catalog", "dbdemos_steventan", "Output Catalog")
dbutils.widgets.text("output_schema", "waf", "Output Schema")

# VM Generation Thresholds by Series (minimum recommended generation)
dbutils.widgets.dropdown("vm_threshold_D", "5", ["3", "4", "5", "6"], "D-Series Min Gen (General Purpose)")
dbutils.widgets.dropdown("vm_threshold_E", "5", ["3", "4", "5", "6"], "E-Series Min Gen (Memory Optimized)")
dbutils.widgets.dropdown("vm_threshold_F", "2", ["2", "3"], "F-Series Min Gen (Compute Optimized)")
dbutils.widgets.dropdown("vm_threshold_L", "3", ["2", "3"], "L-Series Min Gen (Storage Optimized)")
dbutils.widgets.dropdown("vm_threshold_M", "2", ["1", "2"], "M-Series Min Gen (Large Memory)")
dbutils.widgets.dropdown("vm_threshold_N", "1", ["1", "2"], "N-Series Min Gen (GPU)")
dbutils.widgets.dropdown("vm_threshold_default", "5", ["3", "4", "5", "6"], "Default Min Gen (Other Series)")

# COMMAND ----------

# Get widget values
lookback_days = int(dbutils.widgets.get("lookback_days"))
latest_lts_version = dbutils.widgets.get("latest_lts_version")
driver_cpu_threshold = int(dbutils.widgets.get("driver_cpu_threshold"))
driver_memory_gb_threshold = int(dbutils.widgets.get("driver_memory_gb_threshold"))
workspace_filter = dbutils.widgets.get("workspace_filter")

# Output location
output_catalog = dbutils.widgets.get("output_catalog")
output_schema = dbutils.widgets.get("output_schema")
output_location = f"{output_catalog}.{output_schema}"

# VM Generation Thresholds by Series
vm_threshold_D = int(dbutils.widgets.get("vm_threshold_D"))
vm_threshold_E = int(dbutils.widgets.get("vm_threshold_E"))
vm_threshold_F = int(dbutils.widgets.get("vm_threshold_F"))
vm_threshold_L = int(dbutils.widgets.get("vm_threshold_L"))
vm_threshold_M = int(dbutils.widgets.get("vm_threshold_M"))
vm_threshold_N = int(dbutils.widgets.get("vm_threshold_N"))
vm_threshold_default = int(dbutils.widgets.get("vm_threshold_default"))

# Build workspace filter clause
if workspace_filter == "ALL":
    workspace_clause = "1=1"  # No filter
else:
    workspace_clause = f"workspace_id = '{workspace_filter}'"

print(f"""
╔══════════════════════════════════════════════════════════════════════╗
║                       CONFIGURATION SUMMARY                           ║
╠══════════════════════════════════════════════════════════════════════╣
║  Lookback Period:           {lookback_days:>5} days                                    ║
║  Latest LTS Version:        {latest_lts_version:>10}                                     ║
║  Max Driver vCPUs:          {driver_cpu_threshold:>5}                                          ║
║  Max Driver Memory (GB):    {driver_memory_gb_threshold:>5}                                          ║
║  Workspace Filter:          {workspace_filter:<20}                         ║
║  Output Location:           {output_location:<30}               ║
╠══════════════════════════════════════════════════════════════════════╣
║                    VM GENERATION THRESHOLDS BY SERIES                 ║
╠══════════════════════════════════════════════════════════════════════╣
║  D-Series (General Purpose):      v{vm_threshold_D}+                                   ║
║  E-Series (Memory Optimized):     v{vm_threshold_E}+                                   ║
║  F-Series (Compute Optimized):    v{vm_threshold_F}+                                   ║
║  L-Series (Storage Optimized):    v{vm_threshold_L}+                                   ║
║  M-Series (Large Memory):         v{vm_threshold_M}+                                   ║
║  N-Series (GPU):                  v{vm_threshold_N}+                                   ║
║  Other Series (Default):          v{vm_threshold_default}+                                   ║
╚══════════════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1️⃣ Outdated Databricks Runtime (DBR) Versions
# MAGIC 
# MAGIC **Issue**: Clusters running on older DBR versions miss out on performance improvements and cost efficiencies.
# MAGIC 
# MAGIC **Recommendation**: 
# MAGIC - For existing clusters: Upgrade to DBR 15.4+ (or latest LTS 17.3)
# MAGIC - For new clusters: Enforce DBR version through cluster policies
# MAGIC 
# MAGIC ### DBR LTS Support Schedule (Official)
# MAGIC | Version | Release Date | End-of-Support Date |
# MAGIC |---------|--------------|---------------------|
# MAGIC | 17.3 LTS | Oct 22, 2025 | Oct 22, 2028 |
# MAGIC | 16.4 LTS | May 9, 2025 | May 9, 2028 |
# MAGIC | 15.4 LTS | Aug 19, 2024 | Aug 19, 2027 |
# MAGIC | 14.3 LTS | Feb 1, 2024 | Feb 1, 2027 |
# MAGIC | 13.3 LTS | Aug 22, 2023 | Aug 22, 2026 |
# MAGIC | 12.2 LTS | Mar 1, 2023 | Mar 1, 2026 |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📅 DBR LTS Support Reference
# MAGIC 
# MAGIC The cell below creates a reference table (`dbr_lts_support`) containing official end-of-support dates for each LTS version. This is used to calculate how many days remain until each version expires.

# COMMAND ----------

# DBR LTS End-of-Support Reference Table
from datetime import datetime, date
from pyspark.sql.types import StructType, StructField, StringType, DateType

# Official DBR LTS Support Schedule
dbr_lts_support = [
    ("17.3", "2025-10-22", "2028-10-22", "4.0.0"),
    ("16.4", "2025-05-09", "2028-05-09", "3.5.2"),
    ("15.4", "2024-08-19", "2027-08-19", "3.5.0"),
    ("14.3", "2024-02-01", "2027-02-01", "3.5.0"),
    ("13.3", "2023-08-22", "2026-08-22", "3.4.1"),
    ("12.2", "2023-03-01", "2026-03-01", "3.3.2"),
]

# Create DataFrame and register as temp view
schema = StructType([
    StructField("lts_version", StringType(), False),
    StructField("release_date", StringType(), False),
    StructField("end_of_support", StringType(), False),
    StructField("spark_version", StringType(), False),
])

dbr_support_df = spark.createDataFrame(dbr_lts_support, schema)
dbr_support_df.createOrReplaceTempView("dbr_lts_support")

# Display the reference table with days remaining
today = date.today()

print("╔══════════════════════════════════════════════════════════════════════════════════╗")
print("║                        DBR LTS END-OF-SUPPORT REFERENCE                           ║")
print("╠═══════════╦════════════════╦═══════════════════╦════════════════╦═════════════════╣")
print("║  Version  ║  Release Date  ║  End-of-Support   ║  Days Left     ║  Status         ║")
print("╠═══════════╬════════════════╬═══════════════════╬════════════════╬═════════════════╣")

for row in dbr_lts_support:
    version, release_date, end_of_support, spark_ver = row
    eos_date = datetime.strptime(end_of_support, "%Y-%m-%d").date()
    days_left = (eos_date - today).days
    
    if days_left < 0:
        status = "⛔ EXPIRED"
    elif days_left < 180:  # Less than 6 months
        status = "🔴 CRITICAL"
    elif days_left < 365:  # Less than 1 year
        status = "🟠 WARNING"
    else:
        status = "🟢 SUPPORTED"
    
    print(f"║  {version:>7} ║  {release_date:^12}  ║  {end_of_support:^15}  ║  {days_left:>10} d  ║  {status:<14} ║")

print("╚═══════════╩════════════════╩═══════════════════╩════════════════╩═════════════════╝")
print("\n🔴 CRITICAL: < 6 months to end-of-support | 🟠 WARNING: < 1 year | 🟢 SUPPORTED: > 1 year")
print("\n✅ Created temp view: dbr_lts_support")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔎 Cluster-Level DBR Analysis
# MAGIC 
# MAGIC This query identifies **each cluster** using an outdated or non-LTS DBR version. Results include:
# MAGIC - Account and workspace identifiers
# MAGIC - DBR version and support status
# MAGIC - Days until end-of-support
# MAGIC - Cost impact (DBUs and USD)
# MAGIC 
# MAGIC Results are sorted by urgency (Non-LTS and expiring versions first) then by cost.

# COMMAND ----------

# Find clusters with outdated DBR versions - with cost data (DBUs and $)
outdated_dbr_query = f"""
WITH cluster_dbr AS (
    SELECT 
        c.*,
        REGEXP_EXTRACT(c.dbr_version, '([0-9]+\\.[0-9]+)', 1) AS dbr_major_minor
    FROM system.compute.clusters c
    WHERE c.delete_time IS NULL
        AND c.change_time >= date_sub(current_date(), {lookback_days})
        AND {workspace_clause}
),
cluster_with_support AS (
    SELECT 
        cd.*,
        s.lts_version,
        s.end_of_support,
        DATEDIFF(DATE(s.end_of_support), current_date()) AS days_until_eos
    FROM cluster_dbr cd
    LEFT JOIN dbr_lts_support s 
        ON cd.dbr_major_minor = s.lts_version
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
    cws.account_id,
    cws.workspace_id,
    cws.cluster_id,
    cws.cluster_name,
    cws.owned_by AS owner,
    cws.dbr_version,
    COALESCE(cws.lts_version, cws.dbr_major_minor) AS dbr_family,
    CASE WHEN cws.lts_version IS NOT NULL THEN 'LTS' ELSE 'Non-LTS' END AS version_type,
    CAST(cws.end_of_support AS STRING) AS end_of_support_date,
    cws.days_until_eos,
    CASE 
        WHEN cws.lts_version IS NULL THEN '🔴 CRITICAL (Non-LTS)'
        WHEN cws.days_until_eos < 0 THEN '⛔ EXPIRED'
        WHEN cws.days_until_eos < 180 THEN '🔴 CRITICAL (<6 months)'
        WHEN cws.days_until_eos < 365 THEN '🟠 WARNING (<1 year)'
        ELSE '🟢 SUPPORTED'
    END AS support_status,
    cws.driver_node_type,
    cws.worker_node_type,
    cws.cluster_source,
    COALESCE(cc.total_dbus, 0) AS total_dbus,
    COALESCE(cc.total_cost_usd, 0) AS total_cost_usd
FROM cluster_with_support cws
LEFT JOIN cluster_costs cc ON cws.cluster_id = cc.cluster_id
ORDER BY 
    CASE 
        WHEN cws.lts_version IS NULL THEN -1
        WHEN cws.days_until_eos IS NULL THEN 9999 
        ELSE cws.days_until_eos 
    END ASC,
    cc.total_cost_usd DESC NULLS LAST
"""
display(spark.sql(outdated_dbr_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 DBR Version Distribution Summary
# MAGIC 
# MAGIC This aggregated view shows how clusters are distributed across DBR versions, along with:
# MAGIC - Total cluster count per version
# MAGIC - Aggregate DBU consumption and cost
# MAGIC - Percentage of total spend

# COMMAND ----------

# DBR Version Distribution Summary with Cost Impact
dbr_distribution_query = f"""
WITH cluster_dbr AS (
    SELECT 
        c.cluster_id,
        c.workspace_id,
        c.dbr_version,
        REGEXP_EXTRACT(c.dbr_version, '([0-9]+\\.[0-9]+)', 1) AS dbr_major_minor
    FROM system.compute.clusters c
    WHERE c.delete_time IS NULL
        AND c.change_time >= date_sub(current_date(), {lookback_days})
        AND {workspace_clause}
),
cluster_with_support AS (
    SELECT 
        cd.cluster_id,
        cd.workspace_id,
        cd.dbr_version,
        COALESCE(s.lts_version, cd.dbr_major_minor) AS dbr_family,
        s.end_of_support,
        DATEDIFF(DATE(s.end_of_support), current_date()) AS days_remaining
    FROM cluster_dbr cd
    LEFT JOIN dbr_lts_support s ON cd.dbr_major_minor = s.lts_version
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
    CONCAT('DBR ', cws.dbr_family) AS dbr_version,
    CASE WHEN cws.end_of_support IS NULL THEN 'Non-LTS (Short Support)' ELSE CAST(DATE(cws.end_of_support) AS STRING) END AS end_of_support_date,
    COALESCE(cws.days_remaining, 0) AS days_remaining,
    CASE 
        WHEN cws.end_of_support IS NULL THEN '🔴 CRITICAL (Non-LTS)'
        WHEN cws.days_remaining < 0 THEN '⛔ EXPIRED'
        WHEN cws.days_remaining < 180 THEN '🔴 CRITICAL'
        WHEN cws.days_remaining < 365 THEN '🟠 WARNING'
        ELSE '🟢 SUPPORTED'
    END AS status,
    COUNT(DISTINCT cws.cluster_id) AS cluster_count,
    ROUND(SUM(COALESCE(cc.total_dbus, 0)), 2) AS total_dbus,
    ROUND(SUM(COALESCE(cc.total_cost_usd, 0)), 2) AS total_cost_usd,
    ROUND(SUM(COALESCE(cc.total_cost_usd, 0)) * 100.0 / NULLIF(SUM(SUM(COALESCE(cc.total_cost_usd, 0))) OVER (), 0), 2) AS pct_of_total_cost
FROM cluster_with_support cws
LEFT JOIN cluster_costs cc ON cws.cluster_id = cc.cluster_id
GROUP BY cws.dbr_family, cws.end_of_support, cws.days_remaining
ORDER BY 
    CASE WHEN cws.end_of_support IS NULL THEN -1 ELSE COALESCE(cws.days_remaining, 9999) END ASC
"""
display(spark.sql(dbr_distribution_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2️⃣ Older Azure VM Generations
# MAGIC 
# MAGIC **Issue**: Some clusters are running on older VM generations which are less cost-efficient and offer lower performance per dollar.
# MAGIC 
# MAGIC **Recommendation**:
# MAGIC - Upgrade VMs to meet the minimum generation threshold per series
# MAGIC - Upgrading typically improves price/performance
# MAGIC - Different VM generation should work seamlessly (lowest hanging fruit!)
# MAGIC 
# MAGIC **How we detect VM generation**: Extract from node type name (e.g., `Standard_D16s_v3` → Series: D, Gen: v3`)
# MAGIC 
# MAGIC **Thresholds are configurable per series** - see widgets above for current settings

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔎 Cluster-Level VM Generation Analysis
# MAGIC 
# MAGIC This query identifies clusters using **older VM generations** based on series-specific thresholds.
# MAGIC 
# MAGIC **How it works:**
# MAGIC 1. Extracts VM series (D, E, F, etc.) and generation (v3, v4, v5) from node type names
# MAGIC 2. Compares against the configured threshold for that series
# MAGIC 3. Flags clusters where driver OR worker is below threshold
# MAGIC 
# MAGIC Results show both the current generation and recommended minimum for each cluster.

# COMMAND ----------

# Find clusters with older VM generations - with cost data (using series-specific thresholds)
old_vm_query = f"""
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
),
clusters_with_vm_info AS (
    SELECT 
        c.*,
        -- Extract VM series (D, E, F, L, M, N, etc.)
        REGEXP_EXTRACT(c.driver_node_type, 'Standard_([A-Z]+)', 1) AS driver_series,
        REGEXP_EXTRACT(c.worker_node_type, 'Standard_([A-Z]+)', 1) AS worker_series,
        -- Extract VM generation
        TRY_CAST(REGEXP_EXTRACT(c.driver_node_type, '_v([0-9]+)', 1) AS INT) AS driver_gen,
        TRY_CAST(REGEXP_EXTRACT(c.worker_node_type, '_v([0-9]+)', 1) AS INT) AS worker_gen
    FROM system.compute.clusters c
    WHERE c.delete_time IS NULL
        AND c.change_time >= date_sub(current_date(), {lookback_days})
        AND {workspace_clause}
),
clusters_with_thresholds AS (
    SELECT 
        cv.*,
        -- Get threshold based on driver series
        CASE 
            WHEN cv.driver_series LIKE 'D%' THEN {vm_threshold_D}
            WHEN cv.driver_series LIKE 'E%' THEN {vm_threshold_E}
            WHEN cv.driver_series LIKE 'F%' THEN {vm_threshold_F}
            WHEN cv.driver_series LIKE 'L%' THEN {vm_threshold_L}
            WHEN cv.driver_series LIKE 'M%' THEN {vm_threshold_M}
            WHEN cv.driver_series LIKE 'N%' THEN {vm_threshold_N}
            ELSE {vm_threshold_default}
        END AS driver_threshold,
        -- Get threshold based on worker series
        CASE 
            WHEN cv.worker_series LIKE 'D%' THEN {vm_threshold_D}
            WHEN cv.worker_series LIKE 'E%' THEN {vm_threshold_E}
            WHEN cv.worker_series LIKE 'F%' THEN {vm_threshold_F}
            WHEN cv.worker_series LIKE 'L%' THEN {vm_threshold_L}
            WHEN cv.worker_series LIKE 'M%' THEN {vm_threshold_M}
            WHEN cv.worker_series LIKE 'N%' THEN {vm_threshold_N}
            ELSE {vm_threshold_default}
        END AS worker_threshold
    FROM clusters_with_vm_info cv
)
SELECT 
    ct.account_id,
    ct.workspace_id,
    ct.cluster_id,
    ct.cluster_name,
    ct.owned_by AS owner,
    ct.driver_node_type,
    ct.driver_series,
    COALESCE(CAST(ct.driver_gen AS STRING), 'N/A') AS driver_vm_generation,
    ct.driver_threshold AS driver_min_recommended,
    ct.worker_node_type,
    ct.worker_series,
    COALESCE(CAST(ct.worker_gen AS STRING), 'N/A') AS worker_vm_generation,
    ct.worker_threshold AS worker_min_recommended,
    CASE 
        WHEN COALESCE(ct.driver_gen, 99) < ct.driver_threshold 
             OR COALESCE(ct.worker_gen, 99) < ct.worker_threshold
        THEN '🔴 Below Recommended'
        ELSE '🟢 Meets Threshold'
    END AS vm_status,
    nt_driver.core_count AS driver_cores,
    ROUND(nt_driver.memory_mb / 1024, 1) AS driver_memory_gb,
    nt_worker.core_count AS worker_cores,
    ROUND(nt_worker.memory_mb / 1024, 1) AS worker_memory_gb,
    ct.dbr_version,
    ct.cluster_source,
    COALESCE(cc.total_dbus, 0) AS total_dbus,
    COALESCE(cc.total_cost_usd, 0) AS total_cost_usd
FROM clusters_with_thresholds ct
LEFT JOIN system.compute.node_types nt_driver 
    ON ct.driver_node_type = nt_driver.node_type
LEFT JOIN system.compute.node_types nt_worker 
    ON ct.worker_node_type = nt_worker.node_type
LEFT JOIN cluster_costs cc ON ct.cluster_id = cc.cluster_id
WHERE (COALESCE(ct.driver_gen, 99) < ct.driver_threshold 
       OR COALESCE(ct.worker_gen, 99) < ct.worker_threshold)
ORDER BY 
    LEAST(COALESCE(ct.driver_gen, 99), COALESCE(ct.worker_gen, 99)),
    cc.total_cost_usd DESC NULLS LAST
"""
display(spark.sql(old_vm_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 VM Generation Distribution by Series
# MAGIC 
# MAGIC This summary shows VM generation usage **grouped by series**, making it easy to see which series have the most legacy VMs.

# COMMAND ----------

# VM Generation Distribution Summary by Series with Cost Impact
vm_distribution_query = f"""
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
),
clusters_with_vm_info AS (
    SELECT 
        c.cluster_id,
        c.driver_node_type,
        -- Extract series (first letter(s) after Standard_)
        REGEXP_EXTRACT(c.driver_node_type, 'Standard_([A-Z]+)', 1) AS vm_series,
        REGEXP_EXTRACT(c.driver_node_type, '_v([0-9]+)', 1) AS vm_gen_str,
        TRY_CAST(REGEXP_EXTRACT(c.driver_node_type, '_v([0-9]+)', 1) AS INT) AS vm_gen_int,
        -- Get threshold based on series
        CASE 
            WHEN REGEXP_EXTRACT(c.driver_node_type, 'Standard_([A-Z]+)', 1) LIKE 'D%' THEN {vm_threshold_D}
            WHEN REGEXP_EXTRACT(c.driver_node_type, 'Standard_([A-Z]+)', 1) LIKE 'E%' THEN {vm_threshold_E}
            WHEN REGEXP_EXTRACT(c.driver_node_type, 'Standard_([A-Z]+)', 1) LIKE 'F%' THEN {vm_threshold_F}
            WHEN REGEXP_EXTRACT(c.driver_node_type, 'Standard_([A-Z]+)', 1) LIKE 'L%' THEN {vm_threshold_L}
            WHEN REGEXP_EXTRACT(c.driver_node_type, 'Standard_([A-Z]+)', 1) LIKE 'M%' THEN {vm_threshold_M}
            WHEN REGEXP_EXTRACT(c.driver_node_type, 'Standard_([A-Z]+)', 1) LIKE 'N%' THEN {vm_threshold_N}
            ELSE {vm_threshold_default}
        END AS min_threshold
    FROM system.compute.clusters c
    WHERE c.delete_time IS NULL
        AND c.change_time >= date_sub(current_date(), {lookback_days})
        AND {workspace_clause}
        AND REGEXP_EXTRACT(c.driver_node_type, '_v([0-9]+)', 1) IS NOT NULL
        AND REGEXP_EXTRACT(c.driver_node_type, '_v([0-9]+)', 1) != ''
)
SELECT 
    cwi.vm_series AS series,
    CONCAT('v', cwi.vm_gen_str) AS vm_generation,
    CONCAT('v', cwi.min_threshold) AS min_recommended,
    COUNT(DISTINCT cwi.cluster_id) AS cluster_count,
    CASE 
        WHEN cwi.vm_gen_int < cwi.min_threshold THEN '🔴 Below Threshold'
        ELSE '🟢 Meets Threshold'
    END AS status,
    ROUND(SUM(COALESCE(cc.total_dbus, 0)), 2) AS total_dbus,
    ROUND(SUM(COALESCE(cc.total_cost_usd, 0)), 2) AS total_cost_usd,
    ROUND(SUM(COALESCE(cc.total_cost_usd, 0)) * 100.0 / NULLIF(SUM(SUM(COALESCE(cc.total_cost_usd, 0))) OVER (), 0), 2) AS pct_of_total_cost
FROM clusters_with_vm_info cwi
LEFT JOIN cluster_costs cc ON cwi.cluster_id = cc.cluster_id
GROUP BY cwi.vm_series, cwi.vm_gen_str, cwi.vm_gen_int, cwi.min_threshold
ORDER BY cwi.vm_series, cwi.vm_gen_int
"""
display(spark.sql(vm_distribution_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3️⃣ Oversized Driver Nodes
# MAGIC 
# MAGIC **Issue**: Some workloads are configured with driver nodes that are larger than necessary, resulting in unnecessary compute cost.
# MAGIC 
# MAGIC **Recommendation**:
# MAGIC - Right-size drivers in non-prod, then rollout to production
# MAGIC - Enforce driver sizing through cluster policies
# MAGIC - Right-sizing drivers can significantly reduce costs

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔎 Cluster-Level Driver Analysis
# MAGIC 
# MAGIC This query finds clusters where the **driver node exceeds recommended specifications**.
# MAGIC 
# MAGIC **What we check:**
# MAGIC - Driver vCPUs > configured threshold (default: 16)
# MAGIC - Driver memory > configured threshold (default: 64 GB)
# MAGIC 
# MAGIC Large drivers are often unnecessary because the driver primarily coordinates work - the workers do the heavy lifting.

# COMMAND ----------

# Find clusters with oversized driver nodes - with cost data
oversized_driver_query = f"""
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
    c.driver_node_type,
    nt.core_count AS driver_vcpus,
    ROUND(nt.memory_mb / 1024, 1) AS driver_memory_gb,
    CASE 
        WHEN nt.core_count > {driver_cpu_threshold} THEN '🔴 vCPUs Too High'
        WHEN nt.memory_mb / 1024 > {driver_memory_gb_threshold} THEN '🟠 Memory Too High'
        ELSE '🟢 Appropriately Sized'
    END AS sizing_status,
    c.worker_node_type,
    c.worker_count,
    c.dbr_version,
    c.cluster_source,
    COALESCE(cc.total_dbus, 0) AS total_dbus,
    COALESCE(cc.total_cost_usd, 0) AS total_cost_usd
FROM system.compute.clusters c
JOIN system.compute.node_types nt 
    ON c.driver_node_type = nt.node_type
LEFT JOIN cluster_costs cc ON c.cluster_id = cc.cluster_id
WHERE c.delete_time IS NULL
    AND c.change_time >= date_sub(current_date(), {lookback_days})
    AND {workspace_clause}
    AND (nt.core_count > {driver_cpu_threshold} OR nt.memory_mb / 1024 > {driver_memory_gb_threshold})
ORDER BY cc.total_cost_usd DESC NULLS LAST, nt.core_count DESC, nt.memory_mb DESC
"""
display(spark.sql(oversized_driver_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📊 Driver Sizing Summary
# MAGIC 
# MAGIC This summary groups oversized drivers by node type, showing the total cost impact and recommended alternatives.

# COMMAND ----------

# Driver sizing summary with cost impact
driver_sizing_summary_query = f"""
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
    c.driver_node_type,
    nt.core_count AS driver_vcpus,
    ROUND(nt.memory_mb / 1024, 1) AS driver_memory_gb,
    COUNT(DISTINCT c.cluster_id) AS cluster_count,
    ROUND(SUM(COALESCE(cc.total_dbus, 0)), 2) AS total_dbus,
    ROUND(SUM(COALESCE(cc.total_cost_usd, 0)), 2) AS total_cost_usd,
    ROUND(SUM(COALESCE(cc.total_cost_usd, 0)) * 100.0 / NULLIF(SUM(SUM(COALESCE(cc.total_cost_usd, 0))) OVER (), 0), 2) AS pct_of_total_cost,
    CASE 
        WHEN nt.core_count > 32 THEN 'Consider Standard_E8ds_v5 (8 vCPU, 64GB)'
        WHEN nt.core_count > 16 THEN 'Consider Standard_E4ds_v5 (4 vCPU, 32GB)'
        ELSE 'Review workload requirements'
    END AS recommendation
FROM system.compute.clusters c
JOIN system.compute.node_types nt 
    ON c.driver_node_type = nt.node_type
LEFT JOIN cluster_costs cc ON c.cluster_id = cc.cluster_id
WHERE c.delete_time IS NULL
    AND c.change_time >= date_sub(current_date(), {lookback_days})
    AND {workspace_clause}
    AND (nt.core_count > {driver_cpu_threshold} OR nt.memory_mb / 1024 > {driver_memory_gb_threshold})
GROUP BY c.driver_node_type, nt.core_count, nt.memory_mb
ORDER BY total_cost_usd DESC
"""
display(spark.sql(driver_sizing_summary_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4️⃣ Executive Summary
# MAGIC 
# MAGIC This table aggregates all findings into a single view, showing:
# MAGIC - **Issue Category**: Type of optimization opportunity
# MAGIC - **Affected Clusters**: Number of clusters with this issue
# MAGIC - **Total DBUs**: DBU consumption from affected clusters
# MAGIC - **Total Cost (USD)**: Dollar cost based on list prices
# MAGIC - **Recommendation**: Suggested action
# MAGIC 
# MAGIC > 💡 **Tip**: Focus on high-cost items first for maximum impact!

# COMMAND ----------

# Generate executive summary with cost data
from pyspark.sql import functions as F

summary_data = []

# 1. DBR Critical (< 6 months to EOS OR Non-LTS) - with cost
try:
    critical_dbr_df = spark.sql(f"""
        WITH cluster_dbr AS (
            SELECT 
                c.cluster_id,
                REGEXP_EXTRACT(c.dbr_version, '([0-9]+\\.[0-9]+)', 1) AS dbr_major_minor
            FROM system.compute.clusters c
            WHERE c.delete_time IS NULL
                AND c.change_time >= date_sub(current_date(), {lookback_days})
                AND {workspace_clause}
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
            GROUP BY u.usage_metadata.cluster_id
        )
        SELECT 
            COUNT(DISTINCT cd.cluster_id) as cluster_count,
            ROUND(SUM(COALESCE(cc.total_dbus, 0)), 2) as total_dbus,
            ROUND(SUM(COALESCE(cc.total_cost_usd, 0)), 2) as total_cost_usd
        FROM cluster_dbr cd
        LEFT JOIN dbr_lts_support s ON cd.dbr_major_minor = s.lts_version
        LEFT JOIN cluster_costs cc ON cd.cluster_id = cc.cluster_id
        WHERE s.lts_version IS NULL
           OR DATEDIFF(DATE(s.end_of_support), current_date()) < 180
    """)
    row = critical_dbr_df.collect()[0]
    summary_data.append(("🔴 DBR Critical (Non-LTS or <6mo to EOS)", row['cluster_count'], row['total_dbus'], row['total_cost_usd'], "Immediate upgrade to LTS required"))
except Exception as e:
    print(f"Note: Could not query DBR versions - {e}")

# 2. DBR Warning (< 1 year to end-of-support) - with cost
try:
    warning_dbr_df = spark.sql(f"""
        WITH cluster_dbr AS (
            SELECT 
                c.cluster_id,
                REGEXP_EXTRACT(c.dbr_version, '([0-9]+\\.[0-9]+)', 1) AS dbr_major_minor
            FROM system.compute.clusters c
            WHERE c.delete_time IS NULL
                AND c.change_time >= date_sub(current_date(), {lookback_days})
                AND {workspace_clause}
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
            GROUP BY u.usage_metadata.cluster_id
        )
        SELECT 
            COUNT(DISTINCT cd.cluster_id) as cluster_count,
            ROUND(SUM(COALESCE(cc.total_dbus, 0)), 2) as total_dbus,
            ROUND(SUM(COALESCE(cc.total_cost_usd, 0)), 2) as total_cost_usd
        FROM cluster_dbr cd
        JOIN dbr_lts_support s ON cd.dbr_major_minor = s.lts_version
        LEFT JOIN cluster_costs cc ON cd.cluster_id = cc.cluster_id
        WHERE DATEDIFF(DATE(s.end_of_support), current_date()) BETWEEN 180 AND 365
    """)
    row = warning_dbr_df.collect()[0]
    summary_data.append(("🟠 DBR Warning (<1 year to EOS)", row['cluster_count'], row['total_dbus'], row['total_cost_usd'], "Plan upgrade within 6 months"))
except Exception as e:
    print(f"Note: Could not query DBR versions - {e}")

# 3. Old VM Generation - with cost (using series-specific thresholds)
try:
    old_vm_df = spark.sql(f"""
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
            GROUP BY u.usage_metadata.cluster_id
        ),
        clusters_with_threshold AS (
            SELECT 
                c.cluster_id,
                TRY_CAST(REGEXP_EXTRACT(c.driver_node_type, '_v([0-9]+)', 1) AS INT) AS driver_gen,
                TRY_CAST(REGEXP_EXTRACT(c.worker_node_type, '_v([0-9]+)', 1) AS INT) AS worker_gen,
                CASE 
                    WHEN REGEXP_EXTRACT(c.driver_node_type, 'Standard_([A-Z]+)', 1) LIKE 'D%' THEN {vm_threshold_D}
                    WHEN REGEXP_EXTRACT(c.driver_node_type, 'Standard_([A-Z]+)', 1) LIKE 'E%' THEN {vm_threshold_E}
                    WHEN REGEXP_EXTRACT(c.driver_node_type, 'Standard_([A-Z]+)', 1) LIKE 'F%' THEN {vm_threshold_F}
                    WHEN REGEXP_EXTRACT(c.driver_node_type, 'Standard_([A-Z]+)', 1) LIKE 'L%' THEN {vm_threshold_L}
                    WHEN REGEXP_EXTRACT(c.driver_node_type, 'Standard_([A-Z]+)', 1) LIKE 'M%' THEN {vm_threshold_M}
                    WHEN REGEXP_EXTRACT(c.driver_node_type, 'Standard_([A-Z]+)', 1) LIKE 'N%' THEN {vm_threshold_N}
                    ELSE {vm_threshold_default}
                END AS driver_threshold,
                CASE 
                    WHEN REGEXP_EXTRACT(c.worker_node_type, 'Standard_([A-Z]+)', 1) LIKE 'D%' THEN {vm_threshold_D}
                    WHEN REGEXP_EXTRACT(c.worker_node_type, 'Standard_([A-Z]+)', 1) LIKE 'E%' THEN {vm_threshold_E}
                    WHEN REGEXP_EXTRACT(c.worker_node_type, 'Standard_([A-Z]+)', 1) LIKE 'F%' THEN {vm_threshold_F}
                    WHEN REGEXP_EXTRACT(c.worker_node_type, 'Standard_([A-Z]+)', 1) LIKE 'L%' THEN {vm_threshold_L}
                    WHEN REGEXP_EXTRACT(c.worker_node_type, 'Standard_([A-Z]+)', 1) LIKE 'M%' THEN {vm_threshold_M}
                    WHEN REGEXP_EXTRACT(c.worker_node_type, 'Standard_([A-Z]+)', 1) LIKE 'N%' THEN {vm_threshold_N}
                    ELSE {vm_threshold_default}
                END AS worker_threshold
            FROM system.compute.clusters c
            WHERE c.delete_time IS NULL
                AND c.change_time >= date_sub(current_date(), {lookback_days})
                AND {workspace_clause}
        )
        SELECT 
            COUNT(DISTINCT ct.cluster_id) as cluster_count,
            ROUND(SUM(COALESCE(cc.total_dbus, 0)), 2) as total_dbus,
            ROUND(SUM(COALESCE(cc.total_cost_usd, 0)), 2) as total_cost_usd
        FROM clusters_with_threshold ct
        LEFT JOIN cluster_costs cc ON ct.cluster_id = cc.cluster_id
        WHERE COALESCE(ct.driver_gen, 99) < ct.driver_threshold
           OR COALESCE(ct.worker_gen, 99) < ct.worker_threshold
    """)
    row = old_vm_df.collect()[0]
    summary_data.append(("🔸 Older VM Generations (Below Threshold)", row['cluster_count'], row['total_dbus'], row['total_cost_usd'], "Upgrade VMs to recommended generation"))
except Exception as e:
    print(f"Note: Could not query VM generations - {e}")

# 4. Oversized Drivers - with cost
try:
    oversized_driver_df = spark.sql(f"""
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
            GROUP BY u.usage_metadata.cluster_id
        )
        SELECT 
            COUNT(DISTINCT c.cluster_id) as cluster_count,
            ROUND(SUM(COALESCE(cc.total_dbus, 0)), 2) as total_dbus,
            ROUND(SUM(COALESCE(cc.total_cost_usd, 0)), 2) as total_cost_usd
        FROM system.compute.clusters c
        JOIN system.compute.node_types nt ON c.driver_node_type = nt.node_type
        LEFT JOIN cluster_costs cc ON c.cluster_id = cc.cluster_id
        WHERE c.delete_time IS NULL
            AND c.change_time >= date_sub(current_date(), {lookback_days})
            AND {workspace_clause}
            AND (nt.core_count > {driver_cpu_threshold} OR nt.memory_mb / 1024 > {driver_memory_gb_threshold})
    """)
    row = oversized_driver_df.collect()[0]
    summary_data.append(("🔸 Oversized Driver Nodes", row['cluster_count'], row['total_dbus'], row['total_cost_usd'], "Right-size driver configurations"))
except Exception as e:
    print(f"Note: Could not query driver sizes - {e}")

# Create summary DataFrame
if summary_data:
    summary_df = spark.createDataFrame(summary_data, ["Issue Category", "Affected Clusters", "Total DBUs", "Total Cost (USD)", "Recommendation"])
    display(summary_df)
else:
    print("Unable to generate summary - please check system table access permissions")

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
# MAGIC - ❌ Python UDFs, pandas operations
# MAGIC - ❌ ML training with sklearn, torch, etc.
# MAGIC 
# MAGIC **Trade-off**: Photon has a higher DBU rate but typically provides 2-8x performance improvement for compatible workloads.

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
            WHEN LOWER(c.dbr_version) LIKE '%ml%' THEN '⚠️ ML Runtime - evaluate carefully'
            WHEN cpu.avg_cpu_percent >= 70 THEN '🔴 HIGH PRIORITY - Very CPU-bound'
            WHEN cpu.avg_cpu_percent >= 50 THEN '🟠 GOOD CANDIDATE - CPU-bound'
            WHEN cpu.avg_cpu_percent >= 30 THEN '🟡 MODERATE - Some CPU usage'
            ELSE '🟢 LOW PRIORITY - Not CPU-bound'
        END AS photon_priority
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
            ELSE '❌ Likely I/O bound - Photon may not help'
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
        AND LOWER(c.dbr_version) NOT LIKE '%ml%'
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
        WHEN LOWER(c.dbr_version) LIKE '%ml%' THEN '⚠️ ML workloads may not benefit'
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
        WHEN LOWER(c.dbr_version) LIKE '%ml%' THEN '⚠️ Evaluate - ML libraries not accelerated'
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
# MAGIC - [Cluster Policies Best Practices](https://docs.databricks.com/clusters/policy-best-practices.html)
# MAGIC - [Azure VM Pricing](https://azure.microsoft.com/pricing/details/virtual-machines/)
# MAGIC - [Databricks Runtime Release Notes](https://docs.databricks.com/release-notes/runtime/index.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Created for WAF Review**  
# MAGIC **Last Updated**: 10 Dec 2025  
# MAGIC **Author**: Steven Tan (cheeyutcy@gmail.com)
