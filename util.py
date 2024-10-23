
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType, ArrayType,TimestampType
from pyspark.sql.functions import col,from_unixtime,split,trim,lit,to_timestamp,current_timestamp,substring,instr,length,expr
from pyspark.sql import Row
import requests
from datetime import datetime
from google.cloud import bigquery,storage
import json

class Utils():
    """
       Utility class for handling data extraction, transformation, and loading operations.

       Overall Information:
       This class provides methods for:
       - Extracting data from APIs
       - Writing and reading data to/from Google Cloud Storage (GCS)
       - Transforming data into Spark DataFrames
       - Flattening DataFrames
       - Defining schemas for Spark DataFrames and BigQuery
       """
    ## define extractallData function
    def extractallData(self,api_url):
        """
                Extracts data from the specified API URL.

                Parameters:
                    api_url (str): The URL of the API to extract data from.

                Returns:
                    str: A JSON string of the extracted data if successful; otherwise, None.
                """

        ## by using get method extract the data from api
        response = requests.get(api_url)

        ##Check if the request was successful
        if response.status_code == 200:
            ##convert data into json
            all_data = response.json()  # converts the (api)JSON response data into Python data types (usually a dictionary or a list).
            # print("Extracted Data:", data)
            print(f"extract data successfully from {api_url}")
            return json.dumps(all_data)  # Convert the Python dictionary to a JSON string

        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            return None