from main import make_api_call, write_tmp_file, extract_columns, extract_rows, new_df, create_geom_col, write_tmp_csv
from dagster import asset
import datetime as dt
import sys
import time

@asset
def naptan_api():
    # Setting URL and parameter variables
    URL = 'https://naptan.api.dft.gov.uk/v1/access-nodes'
    format = 'csv'
    PARAMS = {'dataFormat': format}
    RETRIES = 3

    # Setting runtime for file suffixes
    RUNTIME = dt.datetime.now().strftime('%Y-%m-%d')

    for n in range(RETRIES):
        print(f'Attempt number {n+1}')
        request = make_api_call(URL, PARAMS)
        if request is None:
            if n == 2:
                sys.exit('The API call is ending due to the HTTP error.')
            else:
                time.sleep(n) #retry after n seconds
                continue
        else:
            break
    
    # Writing temp file
    write_tmp_file(request.text, RUNTIME)

    # Creating DataFrame and adding geom column
    new_df = create_geom_col(new_df(extract_rows(request), extract_columns(request)))
    print(new_df.head())

    # Writing cleansed CSV
    write_tmp_csv(new_df, RUNTIME)

    # In preparation for pushing the data to BigQuery, I would also set the data types of each column.
    # I have refrained from doing this for now.