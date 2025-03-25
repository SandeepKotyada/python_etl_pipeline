1. Objective & Context 
The objective of this assignment is to build a Python data pipeline that prepares a 
dataset for the forecasting team. This dataset will enable them to cluster different 
zip codes in Spain based on meteorological characteristics. 
The forecasting team is responsible for predicting electricity consumption for 
customers 12 months in advance. To improve current predictions, they aim to 
classify zip codes in Spain into 3 to 5 groups based on meteorological data. 
The required dataset includes, for each zip code, power category, and year
month, the number of contracts, the maximum and minimum temperatures, 
and the average relative humidity. The analysis should be performed only on 
contracts with client_type_id equal to 0. Two identical tables are needed: one for 
customers with solar panels and another for customers without solar panels, as 
their consumption behavior may differ significantly. 
The final tables should resemble the example provided: 
Finally, since the forecasting team works directly with the Data Warehouse, the 
resulting datasets must be stored within the database. 
2. Deliverables 
Create one or more code files that accomplish the following: 
1. Ingestion: Gather data from the required datasets:  
a. contracts_eae  
b.  meteo_eae  
c.  zipcode_eae 
Data can be read from a MySQL database or CSV files (you can choose). 
2. Keep only the zip codes with a significant number of contracts: Process 
only zip codes with more than 10 customers. From the meteo_eae table, 
retain only data for zip codes that have more than 10 contracts (in the 
contracts_eae table). 
3. Create the power category classification: Create a new field that classifies 
the power_p1 field from the contracts_eae table into three categories: 
a. Power over 5000 kW.  
b. Power between 3000 kW and 5000 kW. 
c.  Power under 3000 kW 
4. Filter the client type: Retain only records where client_type_id is equal to 0. 
5. Combine the dataframes: Join the meteo_eae table with the contracts_eae 
table. 
6. Solar indicators table: For contracts with solar panels, create a table that 
aggregates data by year, month, zip code, and power category. This table 
should include the maximum temperature, minimum temperature, and 
average relative humidity. This table should have the same structure as the 
example provided in section 1. 
7. Output: Insert the resulting table (from step 6) into a MySQL database (you 
can choose the database name). 
8. Non-Solar customers: Repeat steps 6 and 7 for contracts without solar 
panels. 
For this assignment, consider best practices, overall code coherence, and memory 
management strategies (if applicable); Dask is not required. Thoroughly comment 
on the code to explain each step.

