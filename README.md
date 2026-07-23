# SQL_SRVRMGRTN_TO_ADLS

An end-to-end, metadata-driven data engineering pipeline designed to migrate operational data from an On-Premise SQL Server to Azure Data Lake Storage Gen2 (ADLS Gen2). Utilizes Azure Data Factory (ADF) with Self-Hosted Integration Runtime and a dynamic watermark logic to execute efficient, cost-effective incremental loads in Parquet format.

# 🚀 On-Premise to Cloud Data Migration Pipeline

> Metadata-driven incremental ETL pipeline migrating on-premise SQL Server data to Azure Data Lake Storage Gen2 (ADLS) using Azure Data Factory(ADF).

---

## 🏗 Complete Pipeline Architecture

```text
┌───────────────────────────────┐
│  On-Premise SQL Server (DW)   │
└───────────────┬───────────────┘
                │ (Self-Hosted Integration Runtime)
                ▼
┌───────────────────────────────┐
│     Master Orchestration      │
│     (Parent ADF Pipeline)     │
└───────────────┬───────────────┘
                │
                ├──────► [1] Lookup Activity: Query Metadata from `dbo.ADF_Watermark_Table`
                │
                └──────► [2] ForEach Activity: Pass Parameters Loop
                             │
                             ▼
              ┌──────────────────────────┐
              │ Dynamic Child Pipeline   │
              └──────────────┬───────────┘
                             │
            ┌────────────────┴────────────────┐
            ▼                                 ▼
[3] Lookup MAX(WatermarkColumn)     [4] If Condition Validation
    from Source Table                   (New Records Exist?)
                                              │
                                              ▼ (TRUE)
                                    [5] Copy Activity
                                        Source: Delta Query
                                        Sink: Parquet / Snappy
                                              │
                                              ▼
                                    [6] Stored Procedure:
                                        Update `LastLoadValue`
                                              │
                                              ▼
                             ┌─────────────────────────┐
                             │  Azure Data Lake Gen2   │
                             │ (Date-Partitioned Sink) │
                             └─────────────────────────┘

⚡ Core Logic & Operations

1. Master Pipeline (Metadata Extraction)
Queries control table parameters (TableSchema, TableName, WatermarkColumn, LastLoadValue):

SQL
SELECT TableSchema, TableName, WatermarkColumn, LastLoadValue 
FROM dbo.ADF_Watermark_Table;

2. Child Pipeline (Delta Identification & Extraction)
Calculates current maximum watermark value in the target table:

SQL
SELECT MAX(@{pipeline().parameters.WatermarkColumn}) AS MaxValue
FROM @{pipeline().parameters.TableSchema}.@{pipeline().parameters.TableName}
Executes copy activity using dynamic SQL filtering logic:

SQL
@concat(
  'SELECT * FROM ', pipeline().parameters.TableSchema, '.', pipeline().parameters.TableName,
  if(
    equals(pipeline().parameters.LastLoadValue, null),
    '', 
    concat(' WHERE ', pipeline().parameters.WatermarkColumn, ' > ''', pipeline().parameters.LastLoadValue, '''')
  )
)

3. Watermark Commit
Executes stored procedure [dbo].[UpdateWatermark] to commit state changes upon copy success.

📁 Storage Partitioning Strategy
Target path in ADLS Gen2 is dynamically partitioned by table and daily execution date:

Container Path Expression:
@concat(dataset().Table_Name, '/', formatDateTime(utcNow(), 'yyyy/MM/dd'))

Output File Naming Expression:
@concat(dataset().Table_Name, '_', formatDateTime(utcNow(), 'yyyyMMddHHmmss'), '.parquet')

Plaintext
retaildw/
 └── DimCustomer/
      └── 2026/07/23/
           └── DimCustomer_20260723175500.parquet


🔥 Key Benefits
Cost Effective: Delta extraction drastically cuts compute and network egress overhead.

Fully Dynamic: New source tables are processed automatically by simply registering them in dbo.ADF_Watermark_Table.

State Consistency: Watermark updates commit only after successful Parquet file creation.
