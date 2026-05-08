# Springer Capital - Referral Program Data Pipeline

## Overview
This project profiles referral data, builds a PySpark data pipeline to integrate various operational tables, cleanses text/timezone data, and applies strict business logic to flag potential referral fraud.

## Setup & Requirements
- Docker Desktop installed.  

- Python 3.x (handled via Docker).  

- Input CSV data files placed in the /data directory.  

## Execution Instructions
To build and run the pipeline, execute the following commands in your terminal from the project root:  

1. Build the Docker Image:
docker build -t springer_data_pipeline .

2. Run the Container:
(This command mounts your local output folder so the resulting CSV is saved to your machine):  
docker run -v "${PWD}/output:/app/output" springer_data_pipeline

3. see output folder for the profile results and the referral_report.csv as the final report
## Business Logic
all conditions met and commented on your_script.py


## Data Dictionary
You can refer to the data_dictionary.xls file to see description, data type, and constraints.

## Deployed Web Application for data analysis (Link and Repo)
https://github.com/Jireh5/referral-analytics-web (Repo)

https://referral-analytics-web-s4ft.vercel.app/ (Web App)
