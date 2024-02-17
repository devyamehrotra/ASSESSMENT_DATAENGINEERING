# ASSESSMENT_DATAENGINEERING

This document is designed to be read in parallel with the code in the ASSESSMENT_DATAENGINEERING repository. 


 PROJECT STRUCTURE

```bash

├── .idea
│   ├── .gitignore
│   ├── inspectionProfiles
│   │   ├── Project_Default.xml
│   │   └── profiles_settings.xml
│   ├── misc.xml
│   ├── modules.xml
│   ├── ASSESSMENT_DATAENGINEERING.iml
│   └── workspace.xml
├── .pycache__
│   ├── create_spark.cpython-310.pyc
│   ├── driver.cpython-310.pyc
│   ├── extraction.cpython-310.pyc
│   ├── get_env_variables.cpython-310.pyc
│   ├── ingest.cpython-310.pyc
│   ├── udf.cpython-310.pyc
│   └── validate.cpython-310.pyc
├── Properties
│   ├── Configuration
│   │   └── logging.config
├── source
│   └── oltp
│       └── database.csv
├── output
│   ├── csv_output
│   │   ├── .part-00000-3bd95e79-44ea-48c0-b397-3c954de5b9e9-c000.csv.crc
│   │   ├── ._SUCCESS.crc
│   │   ├── part-00000-3bd95e79-44ea-48c0-b397-3c954de5b9e9-c000.csv
│   │   └── _SUCCESS
│   ├── earthquake_map.html
│   └── parquet_output
│       ├── .part-00000-4a715298-3da8-489b-bf15-aac7e9aac559-c000.snappy.parquet.crc
│       ├── ._SUCCESS.crc
│       ├── part-00000-4a715298-3da8-489b-bf15-aac7e9aac559-c000.snappy.parquet
│       └── _SUCCESS
├── README.md
├── application.log
├── create_spark.py
├── driver.py
├── earthquake_map_adjusted.html
├── extraction.py
├── get_env_variables.py
├── ingest.py
├── main.py
├── requirements.txt
├── udf.py
└── validate.py
```



## Deployment

To deploy this project run driver.py after activating venv

```bash
  python driver.py
```

PLEASE ENSURE SPARK AND HADOOP ARE INSTALLED IN YOUR SYSTEM








## 🚀 About Me
I'm a DATA ENGINEER AT FORWOOD SAFETY..
