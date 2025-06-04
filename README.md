# Data Ingestion Lambda

A serverless AWS Lambda function that processes CSV files from S3 and ingests them into a PostgreSQL database. The service supports both incremental and full data processing with checkpoint management.

## 🔍 Overview

This Lambda function automates the ingestion of Token Consumptions data by:

1. **Scanning S3 buckets** for new CSV files based on date patterns
2. **Processing CSV files** containing information
3. **Inserting data** into PostgreSQL database tables
4. **Managing checkpoints** to track processing progress
5. **Supporting both incremental and full data loads**

The service is designed to run on AWS Lambda with container image deployment, supporting both local development and cloud execution.

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌──────────────────────┐
│   S3 Bucket     │    │  Lambda Function │    │  PostgreSQL DB       │
│                 │    │                  │    │                      │
│ ├─ consumption/ │───▶│ ├─ S3 Processor  │───▶│ ├─ Consumption Data  │
│ ├─ checkpoint/  │    │ ├─ DB Operations │    │ └─ Tables            │
│ └─ CSV files    │    │ └─ Logger        │    │                      │
└─────────────────┘    └──────────────────┘    └──────────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │ AWS Secrets Mgr  │
                       │ (DB Credentials) │
                       └──────────────────┘
```

## ✨ Features

- **Incremental Processing**: Only processes new files since last checkpoint
- **Full Data Load**: Initial run processes all available historical data
- **Checkpoint Management**: Tracks processing progress in S3
- **Error Handling**: Robust error handling with detailed logging
- **Database Integration**: Direct PostgreSQL integration with connection pooling
- **Container Support**: Runs as AWS Lambda container image
- **Local Development**: Full local development support with Docker
- **AWS Secrets Integration**: Secure credential management via AWS Secrets Manager

## 📋 Prerequisites

- **Python 3.12+**
- **Docker** (for containerized development)
- **AWS CLI** configured with appropriate permissions
- **PostgreSQL database** (local or RDS)
- **UV package manager** (for dependency management)

### AWS Permissions Required

- S3: `GetObject`, `ListBucket`, `PutObject`
- Secrets Manager: `GetSecretValue`
- CloudWatch: `CreateLogGroup`, `CreateLogStream`, `PutLogEvents`

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone <repository-url>
cd data-ingestion-lambda
```

### 2. Install Dependencies

Install UV
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Using UV (recommended):
```bash
uv sync
```


### 3. Environment Setup

- Copy the example environment file samople_env.

- Create a `.env` file with the following variables:

```bash
# S3 Configuration
S3_BUCKET=S3_BUCKET_NAME
S3_PREFIX=S3_BUCKET_PREFIX
CHECKPOINT_KEY=checkpoint/last_processed_date.txt

# Database Configuration (Local Development)
DB_NAME=DB_NAME
DB_USER=DB_USER
DB_PASSWORD=DB_PASSWORD
DB_HOST=localhost
DB_PORT=5432 ## for postgress
DB_PORT=localhost ## for local port forwarding

# AWS Configuration
SECRET_NAME=SECRET_NAME
REGION_NAME=AWA_REGION
DEFAULT_DATE=DEFAULT_DATE # 2025_04_30

```

## 🎯 Usage

### Local Development

#### Create Postgres DB
```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres -e POSTGRES_USER=postgres postgres
```

#### Direct Python Execution

```bash
# Ensure your .env file is configured
python main.py
```

### Docker Development

#### 1. Build the Docker Image

```bash
docker build --platform linux/amd64 -t data-ingestion .
```

#### 2. AWS SSO Login

```bash
aws sso login
```

#### 3. Set AWS Profile

```bash
export AWS_PROFILE=AWS_PROFILE
```

#### 4. Run the Container

```bash
docker run -p 9000:8080 \
  -e AWS_PROFILE=$AWS_PROFILE \
  -v ~/.aws:/root/.aws:ro \
  -e S3_BUCKET={BUCKET_NAME}\
  -e S3_PREFIX={BUCKET_PREFIX} \
  -e CHECKPOINT_KEY={CHECKPOINT_KEY} \
  -e MISSING_DATES_KEY={MISSING_DATES_KEY} \
  -e DB_NAME={DB_NAME} \
  -e DB_USER={USER_NAME} \
  -e DB_PASSWORD={DB_PASWORD} \
  -e DB_HOST=host.docker.internal \
  -e DB_PORT={DB_PORT} \
  -e DEFAULT_DATE=2025_04_30\
  -e SECRET_NAME={YOUR_SECRET_NAME}\
  -e REGION_NAME={YOUR_REGION_NAME}\
  data-ingestion
```

#### 5. Invoke the Lambda Function

**Option A: Using cURL**
```bash
curl -X POST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{}'
```

**Option B: Using the Test Script**
```bash
python test_lambda.py
```


**Note**: Make sure to start your database tunnel before running the script locally, and ensure all environment variables are properly configured for your specific deployment environment.