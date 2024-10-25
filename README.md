# Experimenting GCP for Big Data Project 

## Prerequisite

- Account with Google Cloud Platform, Billing enabled
- Create service Account with owner access 
    - id: `dodat-224`
    - Go to manage keys and create `serviceKeyGoogle.json` key, store in this directory  (put in .gitignore)
- Enable APIs
    - Compute Engine API
    - Cloud Dataproc API
    - Cloud Resource Manager API
- Set up virtual environment

        py -m venv env
        ./env/Scripts/activate

- Python libraries
    
        pip install -r requirements.txt

- Download Service Account Key from docs


- Test run

        py setup_test.py

> pip freeze > requirements.txt