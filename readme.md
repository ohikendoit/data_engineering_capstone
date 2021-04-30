# Buidling Datawarehouse of US Visitor Information
### Data Engineering Capstone Project

#### Project Summary
The primary objective of this data engineering capstone project is to utilize what I have learned through the program and implement the stacks and techniques in a problem solving context. Udacity provided datasets have been choosen for the project. The main dataset will be include data on immigration to the United Sates, and supplmentary datasets will include data on airport codes, United States city demographics, and temperature distribution data.

#### Project Structure
```
├── etl                                      # collection of etl scripts 
│   ├── data_cleaning.py                     # data cleaning
│   ├── extract.py                           # data extraction
│   ├── models.py                            # converting dataframe into parquert
│   ├── quality_check.py                     # quality checks
│   └── transform.py                         # data transformation
├── output                                   # output
│   ├── dimensions                           # dimension tables
│   │   ├── airports.parquet                 # airport dimension
│   │   ├── demographics.parquet             # demographics dimension
│   │   └── temperature.parquet              # temperature dimension
│   └── fact                                 # fact tables
│       └── immigrations_fact.parquet        # immigration fact table.
├── sas_data                                 # Immigration data
├── us-cities-demographics.csv               # Demographics data
├── reference.txt                            # Reference list for I94port
├── readme.md                                # Overview
├── airport-codes_csv.csv                    # Airports data
├── Capstone Project Template.ipynb          # Main project notebook 
├── I94_SAS_Labels_Descriptions.SAS          # Immigration data description
├── immigration_data_sample.csv              # Immigration data sample
└── main.py                                  # main functions
```