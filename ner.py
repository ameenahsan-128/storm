import requests
import sys
import os
import re
from pyspark.sql import SparkSession
import json

def extract_emails(text):
    # Regular expression pattern to match email addresses
    pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b'
    
    # Extract emails using the pattern
    emails = re.findall(pattern, text)
    
    return emails

def upload_json_to_hdfs(json_string):
    # Specify the HDFS endpoint and file path
    hdfs_base_url = "http://localhost:9864/webhdfs/v1"
    print(emails)
    email=emails[0]
    file_path = f"/extracted_json/{email[0:-10]}.json"
    hdfs_url = f"{hdfs_base_url}{file_path}"

    # Set the request parameters
    params = {
        "op": "CREATE",
        "user.name": "ubuntu",
        "namenoderpcaddress": "localhost:9000",
        "createflag": "",
        "createparent": "true",
        "overwrite": "true"
    }

    # Make the PUT request to upload the file to HDFS
    response = requests.put(hdfs_url, params=params, data=json_string)

    if response.status_code == 201:
        print(f"Successfully wrote data to HDFS: {hdfs_url}")
    else:
        print(f"Failed to write file to WebHDFS. Status code: {response.status_code}")


# Check if the HDFS file path is provided as an argument
if len(sys.argv) < 2:
    print("Error: HDFS file path argument is missing.")
    print("Usage: spark-submit script.py <hdfs_file_path>")
    sys.exit(1)

# Get the HDFS file path from the command line argument
hdfs_file_path = sys.argv[1]

# Set the SPARK_LOCAL_IP environment variable to bind to localhost
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Initialize SparkSession
spark = SparkSession.builder.appName("Upload JSON to HDFS").getOrCreate()

try:
    # Read the file from HDFS using Spark
    text_df = spark.read.text(hdfs_file_path)
    
    # Extract emails from the text
    emails = text_df.rdd.flatMap(lambda row: extract_emails(row[0])).collect()
    
    # Convert the list of emails to JSON format
    json_string = json.dumps({"Email": emails})

    # Upload the JSON data to HDFS
    upload_json_to_hdfs(json_string)

except Exception as e:
    print("Error:", str(e))

# Stop the Spark session
spark.stop()
