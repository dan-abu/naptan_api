import sys
import datetime as dt
import requests
import pandas as pd
import json
import time
from geojson import Point
import csv

## Turn this into an asset

# Extracting CSV
def make_api_call(url, params):
    try:
        r = requests.get(url, params, timeout=5)
        r.raise_for_status()
        print('Successful API call - HTTP Code: ', r.status_code)
        return r
    except requests.exceptions.HTTPError as errh:
        errorJson = json.loads(r.text)
        errorReason = errorJson['errorMessage']
        print(errh)
        print(f'HTTP Error: {r.status_code}. The problem with your request is the following: {errorReason}')

# Writing to tmp/
def write_tmp_file(file, runtime):
    print('Writing raw temp naptan file...')
    try:
        with open(f'/workspaces/dagster-quickstart/tmpedzakjct/storage/raw_naptan_csv{runtime}.csv', 'w', newline='\n') as f:
            writing = csv.writer(f, delimiter=',')
            writing.writerow(file)
            print(f'Temp file has been created: raw_naptan_csv{runtime}.csv')
    except IOError as e:
        sys.exit('I/O error: ', e)

# Extracting columns from CSV
def extract_columns(table):
    headlines = table.text[0:526]
    headlines = headlines.split(',')
    return headlines

# Extracting remaining rows from CSV
def extract_rows(table):
    data = table.text[527:]
    new_data = data.splitlines()
    final_data = []
    for group in new_data:
        final_data.append(group.split(','))
    return final_data

# Create dataframe
def new_df(dataf, cols):
    print('Creating data...')
    df = pd.DataFrame(data=dataf, columns=cols)
    print('Data has been created.')
    return df

# Creating 'geom' column with geojson data
def create_geom_col(table):
    print('Creating new \'geom\' col...')
    coords_cols = ['Latitude', 'Longitude']
    table[coords_cols] = table[coords_cols].apply(pd.to_numeric)
    #dropping empty lat and long rows rather than filling with 0s which will generate erroneous data
    table.dropna(subset=coords_cols, inplace=True)
    table['Co-ords'] = [coordinate for coordinate in zip(table['Latitude'], table['Longitude'])]
    table['geom'] = table['Co-ords'].apply(Point)
    print('New \'geom\' col created.')
    return table

# Writing cleansed CSV
def write_tmp_csv(dataf, runtime):
    print('Saving data as CSV...')
    try:
        dataf.to_csv(f'/workspaces/dagster-quickstart/tmpedzakjct/storage/cleansed_naptan{runtime}.csv', index=False)
        print(f'Data has been stored: cleansed_naptan{runtime}.csv')
    except IOError as e:
        sys.exit('I/O error: ', e)


if __name__ == '__main__':
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