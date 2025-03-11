import sys
import os
import requests
import zipfile
from io import BytesIO
from time import sleep
from pathlib import Path

#Get download file link from web
LAST_UPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

script_dir = Path(__file__).resolve().parent

# Clears the logs file after each run
log_file_path = script_dir.parent / "data_processing" / "logs" / "log.txt"
with open(log_file_path, "w") as f:
    pass

#Download folder path 
DOWNLOAD_FOLDER = "./csv/"
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

def write(content):
    """
    To write log data into the relevant text file.
    """
    # Get the script's current directory
    script_dir = Path(__file__).resolve().parent
    # Move up one level to the parent directory and navigate to "logs/logs.txt"
    log_file_path = script_dir.parent / "data_processing" / "logs" / "logs.txt"

    with open(log_file_path, "a") as f:
        f.write(content + "\n")

def get_latest_gdelt_links():
    """
    Fetches the latest update file and extracts the download URLs.
    :return: List of CSV ZIP file URLs
    """
    response = requests.get(LAST_UPDATE_URL)
    
    if response.status_code != 200:
        write("Failed to fetch lastupdate.txt")
        return []
    
    lines = response.text.strip().split("\n")
    urls = [line.split()[-1] for line in lines if line.endswith(".zip")]
    
    return urls

def download_and_extract(url, out):
    """
    Downloads a ZIP file from the given URL and extracts CSV files.
    :param url: The URL to download
    :return: List of existing file names
    """
    file_name = url.split("/")[-1]
    response = requests.get(url, stream=True)
    
    if response.status_code != 200:
        write(f"Failed to get {url}")
        return
    
    zip_file = zipfile.ZipFile(BytesIO(response.content))
    
    for file in zip_file.namelist():
        if file.lower().endswith("gkg.csv") and file not in out:
            write(f"Extracting: {file}")
            zip_file.extract(file, DOWNLOAD_FOLDER)
            write(f"Completed: {file_name}")
            out.append(file)

    return list(set(out))

#List of output files names
out = []

# Ensure the "src" directory is in sys.path for the driver.
src_path = "./csv/"
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from pyspark.sql import SparkSession
from schemas.gkg_schema import gkg_schema
from etl.parse_gkg import gkg_parser

def run_pipeline(raw_file, parquet_output, json_output):
    """
    Reads a raw GKG CSV file, transforms each line using gkg_parser,
    creates a Spark DataFrame with the defined schema, and writes the output as a single
    Parquet file and a single JSON file.
    """
    spark = SparkSession.builder.appName("Standalone GKG ETL").getOrCreate()

    # Read the raw file as an RDD of lines.
    rdd = spark.sparkContext.textFile(raw_file)
    
    # Apply the transformation using gkg_parser (which splits each line into 27 fields).
    parsed_rdd = rdd.map(lambda line: gkg_parser(line))
    
    # Convert the transformed RDD to a DataFrame using the defined gkg_schema.
    df = spark.createDataFrame(parsed_rdd, schema=gkg_schema)
    
    # Reduce to a single partition so that we get one output file.
    df_single = df.coalesce(1)
    
    # Write as a single Parquet file.
    df_single.write.mode("overwrite").parquet(parquet_output)
    print(f"Pipeline completed. Single Parquet output written to {parquet_output}")
    
    # Write as a single JSON file.
    df_single.write.mode("overwrite").json(json_output)
    print(f"Pipeline completed. Single JSON output written to {json_output}")
    
    spark.stop()

if __name__ == "__main__":
    while True:
        csv_zip_urls = get_latest_gdelt_links()

        if not csv_zip_urls:
            write("No CSV ZIP links found in lastupdate.txt")
        else:
            write(f"Found {len(csv_zip_urls)} files to download...\n")
            for url in csv_zip_urls:
                output = download_and_extract(url, out)
                raw_file_path = src_path + str(output)
                parquet_output_path = "transformed_gkg.parquet"
                json_output_path = "transformed_gkg.json"
                run_pipeline(raw_file_path, parquet_output_path, json_output_path)
        write("Files downloaded and processed. Sleeping for 15 minutes...")
        sleep(15*60)