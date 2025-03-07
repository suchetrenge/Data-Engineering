# Spotify ETL Pipeline with AWS

## Project Overview
This project implements an ETL (Extract, Transform, Load) pipeline using AWS services to fetch data from the Spotify API, transform it, and load it into Snowflake for further analysis in Power BI.

## Architecture Diagram
Below is the architectural workflow of the project:

![ETL Architecture](image.png)

## Workflow Breakdown

### **Extract Phase:**
1. **Trigger**: AWS CloudWatch initiates the extraction process daily.
2. **Data Extraction**: AWS Lambda (written in Python) extracts raw data from the Spotify API.
3. **Storage**: The extracted raw data is stored in an Amazon S3 bucket.

### **Transform Phase:**
4. **Trigger Event**: The object put event in S3 triggers AWS Glue.
5. **Data Transformation**: AWS Glue (with Apache Spark) processes and transforms the raw data.
6. **Storage**: The transformed data is then stored back in Amazon S3.

### **Load Phase:**
7. **Snowpipe Integration**: Snowpipe loads the transformed data into Snowflake.
8. **Power BI Visualization**: The processed data in Snowflake is analyzed using Power BI.

## Technologies Used
- **AWS CloudWatch** - Scheduled event trigger
- **AWS Lambda** - Data extraction from Spotify API
- **Amazon S3** - Data storage (raw and transformed data)
- **AWS Glue (Apache Spark)** - Data transformation
- **Snowflake (Snowpipe)** - Data warehousing
- **Power BI** - Data visualization and reporting

## Setup Instructions
### Prerequisites:
- AWS Account with access to Lambda, Glue, S3, CloudWatch
- Snowflake account
- Power BI (for visualization)

### Steps to Deploy:
1. **Create an S3 bucket** for raw and transformed data.
2. **Configure AWS Lambda** to fetch data from the Spotify API and store it in S3.
3. **Set up AWS Glue** to transform the raw data.
4. **Enable Snowpipe** to load transformed data into Snowflake.
5. **Connect Power BI** to Snowflake for analysis.

## Future Enhancements
- Automate retries in case of failures.
- Implement data validation and monitoring.
- Optimize Glue jobs for better performance.
- Extend API integrations beyond Spotify.

---

## Block Diagram
Below is the project block diagram representation:

```
+-------------------+        +-------------------+        +-------------------+
|  Spotify API     | -----> | AWS Lambda       | -----> | Amazon S3 (raw)  |
+-------------------+        +-------------------+        +-------------------+
                                                   |
                                                   V
                                           +----------------+
                                           | AWS Glue (ETL) |
                                           +----------------+
                                                   |
                                                   V
                                        +----------------------+
                                        | Amazon S3 (processed) |
                                        +----------------------+
                                                   |
                                                   V
                                          +------------------+
                                          | Snowpipe        |
                                          +------------------+
                                                   |
                                                   V
                                            +--------------+
                                            | Snowflake    |
                                            +--------------+
                                                   |
                                                   V
                                            +-------------+
                                            | Power BI    |
                                            +-------------+
```