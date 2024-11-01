import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,GoogleCloudOptions,WorkerOptions
import os
import logging
# from util import Utils
from datetime import datetime
import requests
import json

class ExtractDataFormAPI(beam.DoFn):
    def process (self,ele,api_url):
        import requests
        import json
        response = requests.get(api_url)
        # print(response,type(response)) ##<Response [200]> <class 'requests.models.Response'>
        if response.status_code == 200:
            extracted_data = response.json() #convert the response from the API (which is in JSON format) into a Python dictionary.
            # print(extracted_data,type(extracted_data)) ##dict
            logging.info(f"extract data successfully from {api_url}")
            yield json.dumps(extracted_data)  # Convert the Python dictionary to a JSON string
        else:
            logging.error(f"Failed to retrieve data. Status code: {response.status_code}")
            yield None

class ExtractRequiredDataAndFlatten(beam.DoFn):
    def process(self,json_str):
        import json

        # print(type(data)) #str
        data_dic = json.loads(json_str) ## Convert the string to JSON (Python dictionary) ## print(data_json,type(data_json)) ## dic

        extract_feature_lst = data_dic['features'] ## fetch feature lst from dic
        # print(extract_feature_lst,type(extract_feature_lst)) ##list

        for feature_dict in extract_feature_lst:
            properties_dic =feature_dict['properties']

            ## add geometry cordinate in  properties
            properties_dic["longitude"] = feature_dict["geometry"]["coordinates"][0]
            properties_dic["latitude"] = feature_dict["geometry"]["coordinates"][1]
            properties_dic["depth"] = feature_dict["geometry"]["coordinates"][2]

            yield properties_dic



class AppyTransformation(beam.DoFn):
    def process(self, properties_dic):
        import json
        from datetime import datetime
        # print(type(properties_dic))

        ## conver UNIX timestamps( in milliseconds )to timestamp(Convert milliseconds to seconds and then to readable timestamp)

        try:
            time = float(properties_dic['time']) / 1000   ## Convert milliseconds to seconds
            if time > 0:
                utc_time = datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')
            else:
                utc_time = None  # Set to None
        except Exception as e:
            logging.error(f"error while convertiong time :{e}")
            utc_time = None

        try:
            update = float(properties_dic['updated']) / 1000
            if update > 0:
                utc_updated = datetime.utcfromtimestamp(update).strftime('%Y-%m-%d %H:%M:%S')
            else:
                utc_updated = None  # Set to None or a default value if invalid

        except (ValueError, OSError) as e:
            logging.error(f"error while convertiong update time :{e}")
            utc_updated = None

        ### add column “area” - based on existing “place” column

        place_str = properties_dic.get("place", "")

        # Use 'find' to locate the first occurrence of 'of'
        index_of_of = place_str.find(' of ')

        if index_of_of != -1:
            # Extract substring after 'of'
            area_loc = place_str[index_of_of + len(' of '):].strip()
            # earthquake_data_dic["area"] = area_loc
        else:
            # earthquake_data_dic["area"] = None  # If no 'of' found, set area to None
            area_loc = place_str

        # Add current timestamp
        # Get Current Timestamp:
        insert_date = datetime.now().timestamp() #) # Output: 1729842930.123456 (timestamp- datetime  convert Unix timestamp.)
        # Convert UNIX Timestamp to UTC Datetime
        insert_date_values = datetime.utcfromtimestamp(insert_date).strftime('%Y-%m-%d %H:%M:%S')


        # Prepare dictionary with required fields
        earthquake_dic= {
            "mag": properties_dic.get("mag"),
            "place": properties_dic.get("place"),
            "time": utc_time,
            "updated": utc_updated,
            "tz": properties_dic.get("tz"),
            "url": properties_dic.get("url"),
            "detail": properties_dic.get("detail"),
            "felt": properties_dic.get("felt"),
            "cdi": properties_dic.get("cdi"),
            "mmi": properties_dic.get("mmi"),
            "alert": properties_dic.get("alert"),
            "status": properties_dic.get("status"),
            "tsunami": properties_dic.get("tsunami"),
            "sig": properties_dic.get("sig"),
            "net": properties_dic.get("net"),
            "code": properties_dic.get("code"),
            "ids": properties_dic.get("ids"),
            "sources": properties_dic.get("sources"),
            "types": properties_dic.get("types"),
            "nst": properties_dic.get("nst"),
            "dmin": properties_dic.get("dmin"),
            "rms": properties_dic.get("rms"),
            "gap": properties_dic.get("gap"),
            "magType": properties_dic.get("magType"),
            "type": properties_dic.get("type"),
            "title": properties_dic.get("title"),
            "area": area_loc,
            "latitude": properties_dic.get("latitude"),
            "longitude": properties_dic.get("longitude"),
            "depth": properties_dic.get("depth"),
            "insert_date":insert_date_values
        }

        yield earthquake_dic
schema = {
        "fields": [
            {"name": "mag", "type": "FLOAT"},
            {"name": "place", "type": "STRING"},
            {"name": "time", "type": "TIMESTAMP"},
            {"name": "updated", "type": "TIMESTAMP"},
            {"name": "tz", "type": "INTEGER"},
            {"name": "url", "type": "STRING"},
            {"name": "detail", "type": "STRING"},
            {"name": "felt", "type": "INTEGER"},
            {"name": "cdi", "type": "FLOAT"},
            {"name": "mmi", "type": "FLOAT"},
            {"name": "alert", "type": "STRING"},
            {"name": "status", "type": "STRING"},
            {"name": "tsunami", "type": "INTEGER"},
            {"name": "sig", "type": "INTEGER"},
            {"name": "net", "type": "STRING"},
            {"name": "code", "type": "STRING"},
            {"name": "ids", "type": "STRING"},
            {"name": "sources", "type": "STRING"},
            {"name": "types", "type": "STRING"},
            {"name": "nst", "type": "INTEGER"},
            {"name": "dmin", "type": "FLOAT"},
            {"name": "rms", "type": "FLOAT"},
            {"name": "gap", "type": "FLOAT"},
            {"name": "magType", "type": "STRING"},
            {"name": "type", "type": "STRING"},
            {"name": "title", "type": "STRING"},
            {"name": "area", "type": "STRING"},
            {"name": "latitude", "type": "FLOAT"},
            {"name": "longitude", "type": "FLOAT"},
            {"name": "depth", "type": "FLOAT"},
            {"name": "insert_date", "type": "TIMESTAMP"},
        ]
    }



if __name__ == '__main__':

    os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r"D:\Mohini Data Science\GCP\Python_gcp\spark-learning-431506-160e4ffdff74_key.json"
    option_obj = PipelineOptions()
    google_cloud = option_obj.view_as(GoogleCloudOptions)
    google_cloud.project = 'spark-learning-431506'
    google_cloud.job_name = "earthquake-ingestion-data"
    google_cloud.region = "us-central1"
    google_cloud.staging_location = "gs://dataflow_bk123/stagging_loc"
    google_cloud.temp_location = "gs://dataflow_bk123/temp_loc"

    # Set the runner to Dataflow
    # option_obj.view_as(PipelineOptions).runner = 'DataflowRunner'  # For Dataflow

    # Set up logging
    logging.basicConfig(level=logging.INFO)

    ## API Url
    api_url ="https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

    ###paths
    ## Get the current date and time in 'YYYYMMDD_HHMMSS' format
    cur_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    ## bronze layer location
    landing_location =f'gs://earthquake_analysis_buck/dataflow/landing/{cur_timestamp}/earthquake'
    ## read data from landinng location
    read_data_loc = landing_location +'*.json'
    # print(f'read data path : {read_data_loc}')

    ### write data in gsc silver location
    silver_gcs_loction = f'gs://earthquake_analysis_buck/dataflow/silver/{cur_timestamp}/flatten_earthquake_data'

    ## bq table location
    output_db = 'spark-learning-431506.earthquake_db.dataflow_earthquake_data'



    ## create first pipeline for  extract data from API and write extracted data in gcs(bronze layer)

    with beam.Pipeline(options=option_obj) as p:
        extract_data_api_and_write_gcs = (p| "StartPipeline" >> beam.Create([None])  # Initialize with dummy element
                |'extract data from api'>> beam.ParDo(ExtractDataFormAPI(),api_url)
                |'extracted data write to GCS' >> beam.io.WriteToText(landing_location,file_name_suffix='.json',shard_name_template='')
             # |'print extracted location'>> beam.Map(print)
                      )

        logging.info("pipeline1 get execuated sucessfully")



    ## create second pipeline for read data from landing loction and
    ##fetch required data flatten it and apply transformation and
    ##write data in gcs silver loc and in bq table """

    with beam.Pipeline(options=option_obj) as p2:
        read_data_from_landing_loc_apply_trans = (p2
                                      | 'Read data From GCS landing location' >> beam.io.ReadFromText(read_data_loc)
                                      | 'fetch required data and flatten it'>>beam.ParDo(ExtractRequiredDataAndFlatten())
                                      | 'apply transformation on flatten data'>> beam.ParDo(AppyTransformation())
                                    # | 'PrintReadData' >> beam.Map(print)
                                        )

        (read_data_from_landing_loc_apply_trans
            |'clean data write in gcs silver layer' >> beam.io.WriteToText
                                                    (silver_gcs_loction,file_name_suffix='.json',shard_name_template='')

        )
        (read_data_from_landing_loc_apply_trans | "write into bigquery" >> beam.io.WriteToBigQuery(
                table=output_db,
                schema=schema,  ##schema='SCHEMA_AUTODETECT'
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED )
        )

        logging.info("pipeline2 get execuated sucessfully")



