# User guide

## 1st option -- without python scripts

Follow this order, it is needed since tables are populated in different sql scripts:

 1. 01_setup
 2. 02_create_tables
 3. 03_sample_data
 4. 03a_replace_python_script
 5. 04_etl_functions
 6. 05_optimizations
 7. 06_analytics

## 2nd option -- python script

The same order as above, **but** replace 03a_replace_python_script with generate_data file.
