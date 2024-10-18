# Experimenting GCP for Big Data Project 

## Prerequisite

- Account with Google Cloud Platform, Billing enabled
- Create service Account with owner access 
    - id: `dodat-224`
    - Go to manage keys and create `serviceKeyGoogle.json` key, store in this directory  (put in .gitignore)
- Enable APIs
    - Cloud Dataproc API
- Set up virtual environment

        py -m venv env
        ./env/Scripts/activate

- Python libraries
    
        pip install -r requirements.txt
    
## Project Structure

- gcs - working with Google Cloud Storage
    - `load_data.py` - download and upload data
    - `bucket.py` - create bucket on cloud storage
    - `serviceKeyGoogle.json` - authentication file

## Run

Creating Google Cloud Storage for storing Raw Data

    cd gcs
    py main.py

Create Google Dataproc Cluster for Spark

    cd dataproc
    py main.py