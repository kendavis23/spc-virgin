import functions_framework
import pandas as pd
import io
import datetime
from io import StringIO
from pandas_gbq import to_gbq
from google.cloud import storage
from google.cloud import bigquery
from updateCI import updateci

@functions_framework.http
def main(request):
    
    request_json = request.get_json(silent=True)
    request_args = request.args
    
    bucket = "spc_financials"
    name = "SPC_Virgin.csv"

    df_virgin = read_virgin(bucket, name)

    #buffer = io.StringIO()
    #df_virgin.info(buf=buffer)
    #s = buffer.getvalue()
    #print(s)
    #print(df_virgin.head().to_string())

    rev_df = revenue(df_virgin)
    print(rev_df.head().to_string())
    table_id = 'spc-sandbox-453019.financials.spc-revenue'
    rev_df.to_gbq(table_id, if_exists='replace')

    cost_df = cost(df_virgin)
    print(cost_df.head().to_string())
    table_id = 'spc-sandbox-453019.financials.spc-cost'
    cost_df.to_gbq(table_id, if_exists='replace')

    return 'Hello World!'


def read_virgin(bucket_name, source_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    blob_data = blob.download_as_text()
    data = StringIO(blob_data)
    df = pd.read_csv(data)

    df.columns.values[0] = 'Date'
    df.columns.values[1] = 'Category'
    df.columns.values[2] = 'Xero'
    df.columns.values[3] = 'Description'
    df.columns.values[4] = 'Reconciled'
    df.columns.values[5] = 'Source'
    df.columns.values[6] = 'Amount'
    df.columns.values[7] = 'Balance'

    new_df = df[['Date', 'Description', 'Category', 'Amount']].copy()
    new_df['Ops_Include'] = True
    new_df['Type'] = "Revenue"

    for index, row in new_df.iterrows():
        amount = new_df.loc[index, 'Amount']
        if '(' in amount:
            new_df.loc[index, 'Type'] = "Cost"
        amount = amount.replace('(','')
        amount = amount.replace(')','')
        amount = amount.replace(',','')
        new_df.loc[index, 'Amount'] = amount

    new_df['Date'] = pd.to_datetime(new_df['Date'])
    new_df['Description'] = new_df['Description'].astype(str)
    new_df['Category'] = new_df['Category'].astype(str)
    new_df['Amount'] = new_df['Amount'].astype(float)

    new_df['Category'] = 'other'

    return new_df
    
def revenue (virgin_df):

    rev_df = virgin_df[virgin_df['Type'] == "Revenue"]
    rev_df = rev_df.reset_index(drop=True)

    client = bigquery.Client()
    ex_query = """
    SELECT Substring 
    FROM `spc-sandbox-453019.financials.config-ops-exclude` 
    WHERE File = 'virgin' AND Type = 'revenue'
    """
    ex_df = client.query(ex_query).to_dataframe()

    rc_query = """
    SELECT Substring, Category 
    FROM `spc-sandbox-453019.financials.revenue_categories` 
    WHERE File = 'virgin'
    """
    rc_df = client.query(rc_query).to_dataframe()

    return updateci(rc_df, ex_df, rev_df)

def cost(virgin_df):

    cost_df = virgin_df[virgin_df['Type'] == "Cost"]
    cost_df = cost_df.reset_index(drop=True)

    client = bigquery.Client()
    ex_query = """
    SELECT Substring 
    FROM `spc-sandbox-453019.financials.config-ops-exclude` 
    WHERE File = 'virgin' AND Type = 'cost'
    """
    ex_df = client.query(ex_query).to_dataframe()

    rc_query = """
    SELECT Substring, Category 
    FROM `spc-sandbox-453019.financials.config-cost-categories`
    WHERE File = 'virgin'"""
    rc_df = client.query(rc_query).to_dataframe()

    return updateci(rc_df, ex_df, cost_df)
