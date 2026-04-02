# Electricity Price Data
* For each month from 2017/01 to the latest available month, fetch the zipfile data from the source site via API, extract all csv files inside the zipfile, and concatenate them.
* Create the table in Big Query from the first month(2017/01) only.
* Append data to the table from 2017/02 up to the latest available month.