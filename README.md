# Experimenting GCP for Big Data Project 

## Prerequisite

- Account with Google Cloud Platform
- Create service Account with owner access 
    - id: `dodat-224`
    - Go to manage keys and create `serviceKeyGoogle.json` key, store in this directory  (put in .gitignore)
- Python libraries
    - requests
    - bs4
    - os
    - urllib3
    - google-cloud
    
## Project Structure

- gcs - working with Google Cloud Storage
    - `load_data.py` - download and upload data
    - `bucket.py` - create bucket on cloud storage
    - `serviceKeyGoogle.json` - authentication file

## Run

    cd gcs
    py main.py