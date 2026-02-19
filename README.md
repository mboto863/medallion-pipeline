# Medallion Architecture ELT Pipeline (Serverless Databricks)

A hands-on, end-to-end demonstration of the Medallion Architecture data pipeline, implemented in Databricks. It is purposed as a basic demonstration of the medallion pattern, with three layers:

- **Bronze Layer**: ingestion of raw data with Auto Loader
- **Silver Layer**: schema enforcement, standardizing formats, preparing the data for downstream gold layer logic.
- **Gold Layer**: data in this layer needs to encourage analysis and interpretation, with low effort.

```mermaid
flowchart LR

    classDef raw fill:#9cd6ff,stroke:#4b8dc9,color:black;
    classDef bronze fill:#c97a40,stroke:#8a4f26,color:white;
    classDef silver fill:#bfbfbf,stroke:#7d7d7d,color:black;
    classDef gold fill:#d4af37,stroke:#a88734,color:black;
    classDef job fill:#4caf50,stroke:#2d6e32,color:white;

    A[Cloud Storage<br/>Raw Files]:::raw --> J1
    J1[Databricks Job Task 1<br/>bronze_ingest.py<br/>Serverless]:::job --> B[Bronze Delta Table]:::bronze

    B --> J2
    J2[Databricks Job Task 2<br/>silver_transform.py<br/>Serverless]:::job --> S[Silver Delta Table]:::silver

    S --> J3
    J3[Databricks Job Task 3<br/>gold_aggregations.py<br/>Serverless]:::job --> G[Gold Delta Tables<br/>Analytics / BI]:::gold
```
