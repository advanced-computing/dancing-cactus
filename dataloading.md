# Electricity Price Data
* For each month from 2017/01 to the latest available month, fetch the zipfile data from the source site via API, extract all csv files inside the zipfile, and concatenate them.
* Create the table in Big Query from the first month(2017/01) only.
* Append data to the table from 2017/02 up to the latest available month.
* Using a separate python script, append data from the most recent timestamp in the dataset up to the current date.(This is under constrcution)

# Henry Hub Natural Gas Data
* Use a truncate-and-load (full refresh) strategy.
* For each run, fetch the full daily Henry Hub series from the EIA API, starting from 1993/12/24 up to the latest available date.
* Keep the relevant fields, clean the data, and replace the BigQuery table each time using `if_exists="replace"`.
* This strategy is appropriate because the Henry Hub dataset is relatively small and stable, so a full refresh is simpler and more reliable than incremental loading. It avoids the extra complexity of tracking the last loaded date, handling reruns, and preventing duplicate rows. As a result, the BigQuery table always contains one clean and up-to-date version of the source data.
