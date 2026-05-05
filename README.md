# spotify_azure_project
#Ingestion part done and here i want to show how ingestion pipeline looks like 

<img width="851" height="248" alt="Screenshot 2026-05-04 093321" src="https://github.com/user-attachments/assets/6f5f0198-abb9-41cb-a82e-9df429135dd8" />

# 🎧 Spotify Data Engineering Pipeline (Azure + Databricks)

## 📌 Overview

This project implements a **scalable end-to-end data engineering pipeline** for Spotify data using **Azure Data Factory, Databricks, PySpark, and Delta Lake**.

The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)** to transform raw streaming data into analytics-ready datasets with support for **incremental processing and historical tracking (SCD Type 2)**.

---

## ⚙️ Tech Stack

* **Azure Data Factory (ADF)** – Orchestration & Incremental ingestion
* **Azure Data Lake Storage (ADLS Gen2)** – Storage layer
* **Databricks (PySpark)** – Data processing
* **Delta Lake** – Storage & ACID transactions
* **Delta Live Tables (DLT)** – CDC & SCD Type 2 pipelines
* **Jinja** – Dynamic SQL generation
* **Power BI** – Data visualization

---

## 🏗️ Architecture

### 🔹 Data Ingestion (ADF)

* Built ADF pipeline with:

  * **Lookup (last_cdc)** → fetch last processed timestamp
  * **Copy Activity** → ingest incremental data
  * **ForEach loop** → dynamic table processing
  * **Web Activity (Alerts)** → failure notifications

👉 Supports **incremental loading using CDC column**

---

### 🔹 Bronze Layer (Raw)

* Raw Spotify data stored in ADLS
* Schema applied dynamically using Auto Loader

---

### 🔹 Silver Layer (Cleaned & Transformed)

Implemented using **PySpark Structured Streaming + Auto Loader**

Key transformations:

* Schema evolution (`cloudFiles.schemaEvolutionMode`)
* Deduplication (`dropDuplicates`)
* Data cleaning (e.g., track name normalization)
* Feature engineering:

  * `duration_flag` (low / medium / high)
* Reusable transformation utilities

Example:

```python
df_user = df_user.withColumn("user_name", upper(col("user_name")))
df_user = df_user.dropDuplicates(['user_id'])
```

---

### 🔹 Gold Layer (Business Layer)

Implemented using **Delta Live Tables (DLT)**

* Applied **SCD Type 2** for dimensions:

  * `dimuser`
  * `dimtrack`
  * `dimdate`

* Applied **SCD Type 1** for fact table:

  * `factstream`

Example:

```python
dlt.create_auto_cdc_flow(
    target="dimuser",
    source="dimuser_stg",
    keys=["user_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)
```

---

### 🔹 Data Modeling

* Implemented **Star Schema**

  * Fact Table: `factstream`
  * Dimension Tables: `dimuser`, `dimtrack`, `dimdate`

---

### 🔹 Dynamic Query Generation (Jinja)

Used Jinja to dynamically build SQL joins across fact and dimension tables.

Example:

```sql
SELECT
    factstream.stream_id,
    dimuser.user_name,
    dimtrack.track_name
FROM factstream
LEFT JOIN dimuser ON factstream.user_id = dimuser.user_id
LEFT JOIN dimtrack ON factstream.track_id = dimtrack.track_id
```

---

## 📊 Key Features

* ⚡ Streaming ingestion using Auto Loader
* 🔄 Incremental processing with ADF
* 🧹 Data cleaning & transformation using PySpark
* 📦 Delta Lake with ACID guarantees
* 🕒 Historical tracking using SCD Type 2
* 🧠 Dynamic SQL using Jinja templates
* 🚀 End-to-end pipeline orchestration

---

## 🚀 How to Run

### 1. Deploy using Databricks CLI

```bash
databricks bundle deploy --target dev
```

### 2. Run pipeline

```bash
databricks bundle run
```

### 3. Run tests

```bash
uv run pytest
```

---

## 📈 Future Improvements

* Add data quality checks (Great Expectations / DLT expectations)
* Implement CI/CD with Azure DevOps
* Add real-time dashboarding

---

## 👨‍💻 Author

**Piyush Garg**
