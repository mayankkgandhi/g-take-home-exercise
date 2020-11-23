# ETL take-home assignment

## The Problem Scope
TO buil and RTL pipeline that extracts data from propertypriceregister.ie, transforms the data based on Geowox table structure, data types, naming conventions guidelines and refresh existing data.
An example mapping file can be found at `config/pipeline.json` 

## Solution
The solutiion is to have built an ETL pipeline which is based on json configuration file.
Provide a json configuration file as input to implement the ETL process.


**Run**:
``` shell script
pip install -r requirements.txt
python src/main.py config/pipeline.json
```