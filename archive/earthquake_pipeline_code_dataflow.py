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
            print(f"extract data successfully from {api_url}")
            yield json.dumps(extracted_data)  # Convert the Python dictionary to a JSON string
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
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
    def process(self,earthquake_data_dic ):
        from datetime import datetime

        ## conver UNIX timestamps( in milliseconds )to timestamp(Convert milliseconds to seconds and then to readable timestamp)
        # Convert milliseconds to seconds
        time_seconds = earthquake_data_dic['time'] / 1000.0
        update_seconds = earthquake_data_dic['updated'] / 1000.0

        time_timestamp = datetime.fromtimestamp(time_seconds).strftime('%Y-%m-%d %H:%M:%S')
        update_timestamp = datetime.fromtimestamp(update_seconds).strftime('%Y-%m-%d %H:%M:%S')


        ######### add column “area” - based on existing “place” column

        # Extract the substring after the first occurrence of 'of'
        place_str = earthquake_data_dic.get("place", "")

        # Use 'find' to locate the first occurrence of 'of'
        index_of_of = place_str.find(' of ')

        if index_of_of != -1:
            # Extract substring after 'of'
            area_loc = place_str[index_of_of + len(' of '):].strip()
            # earthquake_data_dic["area"] = area_loc
        else:
            # earthquake_data_dic["area"] = None  # If no 'of' found, set area to None
            area_loc = None


        ### make formated dic
        earthquake_data_dic_formated={}
        earthquake_data_dic_formated['mng'] = str(earthquake_data_dic['mag'])
        earthquake_data_dic_formated['place'] = str(earthquake_data_dic['place'])
        earthquake_data_dic_formated['time'] = time_timestamp
        earthquake_data_dic_formated['updated'] = update_timestamp
        earthquake_data_dic_formated['tz'] = str(earthquake_data_dic['tz'])
        earthquake_data_dic_formated['url'] = str(earthquake_data_dic['url'])
        earthquake_data_dic_formated['detail'] = str(earthquake_data_dic['detail'])
        earthquake_data_dic_formated['felt'] = str(earthquake_data_dic['felt'])
        earthquake_data_dic_formated['cdi'] = str(earthquake_data_dic['cdi'])
        earthquake_data_dic_formated['mmi'] = str(earthquake_data_dic['mmi'])
        earthquake_data_dic_formated['alert'] = str(earthquake_data_dic['alert'])
        earthquake_data_dic_formated['status'] = str(earthquake_data_dic['status'])
        earthquake_data_dic_formated['tsunami'] = int(earthquake_data_dic['tsunami'])
        earthquake_data_dic_formated['sig'] = int(earthquake_data_dic['sig'])
        earthquake_data_dic_formated['net'] = str(earthquake_data_dic['net'])
        earthquake_data_dic_formated['code'] = str(earthquake_data_dic['code'])
        earthquake_data_dic_formated['ids'] = str(earthquake_data_dic['ids'])
        earthquake_data_dic_formated['sources'] = str(earthquake_data_dic['sources'])
        earthquake_data_dic_formated['types'] = str(earthquake_data_dic['types'])
        earthquake_data_dic_formated['nst'] = str(earthquake_data_dic['nst'])
        earthquake_data_dic_formated['dmin'] = str(earthquake_data_dic['dmin'])
        earthquake_data_dic_formated['rms'] = str(earthquake_data_dic['rms'])
        earthquake_data_dic_formated['gap'] = str(earthquake_data_dic['gap'])
        earthquake_data_dic_formated['magType'] = str(earthquake_data_dic['magType'])
        earthquake_data_dic_formated['type'] = str(earthquake_data_dic['type'])
        earthquake_data_dic_formated['title'] = str(earthquake_data_dic['title'])
        earthquake_data_dic_formated['area'] = area_loc
        earthquake_data_dic_formated['longitude'] = float(earthquake_data_dic['longitude'])
        earthquake_data_dic_formated['latitude'] = float(earthquake_data_dic['latitude'])
        earthquake_data_dic_formated['depth'] = float(earthquake_data_dic['depth'])
        earthquake_data_dic_formated['insert_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        yield earthquake_data_dic_formated



if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r"D:\Mohini Data Science\GCP\Python_gcp\spark-learning-431506-160e4ffdff74_key.json"
    option_obj = PipelineOptions()
    google_cloud = option_obj.view_as(GoogleCloudOptions)
    google_cloud.project = 'spark-learning-431506'
    google_cloud.job_name = "earthquake-ingestion-data"
    google_cloud.region = "us-central1"
    google_cloud.staging_location = "gs://dataflow_bk123/stagging_loc"
    google_cloud.temp_location = "gs://dataflow_bk123/temp_loc"
    # option_obj.view_as(WorkerOptions).zone = 'us-central1-b'

    # Set the runner to Dataflow
    # option_obj.view_as(PipelineOptions).runner = 'DataflowRunner'  # For Dataflow

    # Set up logging
    logging.basicConfig(level=logging.INFO)

    ## create util class object
    # util_obj = Utils()

    ## API Url
    api_url ="https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

    ###paths
    ## Get the current date and time in 'YYYYMMDD_HHMMSS' format
    cur_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    ## bronze layer location
    landing_location =f'gs://earthquake_analysis_buck/dataflow/landing/{cur_timestamp}/earthquake'

    ## read data from landinng location
    read_data_loc = landing_location+'*.json'
    # print(f'read data path : {read_data_loc}')

    ### write data in gsc silver location
    silver_gcs_loction = f'gs://earthquake_analysis_buck/dataflow/silver/{cur_timestamp}/flatten_earthquake_data'

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
    output_db = 'spark-learning-431506.earthquake_db.dataflow_earthquake_data'



    ## create pipeline
    ## extract data from API
    ## write extracted data in gcs(bronze layer)

    with beam.Pipeline(options=option_obj) as p:
        extract_data_api_and_write_gcs = (p| "StartPipeline" >> beam.Create([None])  # Initialize with dummy element
                |'extract data from api'>> beam.ParDo(ExtractDataFormAPI(),api_url)
                |'extracted data write to GCS' >> beam.io.WriteToText(landing_location,file_name_suffix='.json',shard_name_template='')
             # |'print extracted location'>> beam.Map(print)
                      )


    ## read data from landing layer

    with beam.Pipeline(options=option_obj) as p2:
        read_data_from_landing_loc_apply_trans = (p2
                                      | 'ReadFromGCS' >> beam.io.ReadFromText(read_data_loc)
                                      | 'read required data and flatten it'>>beam.ParDo(ExtractRequiredDataAndFlatten())
                                      | 'apply transformation on flatten data'>> beam.ParDo(AppyTransformation())
                                    # | 'PrintReadData' >> beam.Map(print)
                                        )

        (read_data_from_landing_loc_apply_trans
            |'clean data write in gcs silver layer' >> beam.io.WriteToText
                                                    (silver_gcs_loction,file_name_suffix='.json',shard_name_template='')

        )
        (read_data_from_landing_loc_apply_trans | "writeintobigquery" >> beam.io.WriteToBigQuery(
                table=output_db,
                schema='SCHEMA_AUTODETECT',  ##schema='SCHEMA_AUTODETECT'
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED )
        )




