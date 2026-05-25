import os
import json
import boto3
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION', 'us-east-1')
)

BUCKET = os.getenv('S3_BUCKET')

def fetch_from_postgres():
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        dbname=os.getenv('POSTGRES_DB', 'sales_analytics'),
        user=os.getenv('POSTGRES_USER', 'admin'),
        password=os.getenv('POSTGRES_PASSWORD', 'secret')
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM raw_sales_events;")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    conn.close()
    return [dict(zip(columns, row)) for row in rows]

def upload_to_s3(data):
    from decimal import Decimal
    timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f"raw-sales/sales_{timestamp}.json"

    for record in data:
        for key, value in record.items():
            if isinstance(value, datetime):
                record[key] = value.isoformat()
            elif isinstance(value, Decimal):
                record[key] = float(value)

    s3.put_object(
        Bucket=BUCKET,
        Key=filename,
        Body=json.dumps(data, indent=2),
        ContentType='application/json'
    )
    print(f"Uploaded {len(data)} records to s3://{BUCKET}/{filename}")

if __name__ == "__main__":
    print("Fetching data from PostgreSQL...")
    data = fetch_from_postgres()
    
    if not data:
        print("No data found in PostgreSQL!")
    else:
        print(f"Found {len(data)} records. Uploading to S3...")
        upload_to_s3(data)
        print("Done!")