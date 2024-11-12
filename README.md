# Experimenting GCP for Big Data Project 

![alt text](docs/pipeline.png)

## Prerequisite

- Account with Google Cloud Platform, Billing enabled (Skip)
- Create service Account with owner access (Skip)
    - id: `tadod-sa-434`
    - Go to manage keys and create `serviceKeyGoogle.json` key, store in this directory  (put in .gitignore)
- Enable APIs (Skip)
    - Compute Engine API
    - Cloud Dataproc API
    - Cloud Resource Manager API
- Set up virtual environment

        py -m venv env
        ./env/Scripts/activate

- Python libraries
    
        pip install -r requirements.txt

- Create Service Account with owner role, create key and save with name `serviceKeyGoogle.json`

- Test run

        py setup_test.py

## Project Structure

- `gcs`
    - `bucket.py` - function to create bucket on Google Cloud Storage
    - `load_data.py` - functions to download data and upload to bucket on GCS
    - `main.py` - execution file

- `dataproc`
    - `cluster.py` - functions to manage dataproc cluster (create, update, delete, start, stop, submit job)
    - `jobs` - contains PySpark jobs
        - `wh_init.py` - init data warehouse on Google BigQuery (year: 2011)
        - `wh_batch_load.py` - batch processing each year from 2012 to present
    - `main.py` - execution file

- `bigquery`

- `docs` - files for README.md
- `setup_test.py` - check authen from local to GCP
- `serviceKeyGoogle.json` - Service Account key for authentication

## Project Infomation

- Project ID: `uber-analysis-439804`
- Region: `us-central1`
- Dataproc's Cluster Name: `uber-hadoop-spark-cluster`
- Bucket's Name: 
    - Raw data: `uber-{year}-154055`
    - PySpark jobs and tmp dir: `uber-pyspark-jobs`

> pip freeze > requirements.txt