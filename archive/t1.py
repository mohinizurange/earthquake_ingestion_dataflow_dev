import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,GoogleCloudOptions,WorkerOptions
import requests
import json
import datetime
import os

schema={
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

class ExtractDataFormAPI(beam.DoFn):
    def process(self, element,api_url):
        import requests
        import json

        # Fetch data from API
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()  # Fetch the entire JSON response
            yield json.dumps(data)  # Yield the full JSON as a single string
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
    def process(self, properties):
        import json
        import datetime
        # print(type(properties))


        # Convert milliseconds to a BigQuery-compatible timestamp
        time = datetime.datetime.fromtimestamp(properties.get("time") / 1000) if properties.get("time") else None
        updated = datetime.datetime.fromtimestamp(properties.get("updated") / 1000) if properties.get(
            "updated") else None

        place_str = properties.get("place", "")

        # Use 'find' to locate the first occurrence of 'of'
        index_of_of = place_str.find(' of ')

        if index_of_of != -1:
            # Extract substring after 'of'
            area_loc = place_str[index_of_of + len(' of '):].strip()
            # earthquake_data_dic["area"] = area_loc
        else:
            # earthquake_data_dic["area"] = None  # If no 'of' found, set area to None
            area_loc = None

        # Prepare dictionary with required fields
        yield {
            "mag": properties.get("mag"),
            "place": properties.get("place"),
            "time": time,
            "updated": updated,
            "tz": properties.get("tz"),
            "url": properties.get("url"),
            "detail": properties.get("detail"),
            "felt": properties.get("felt"),
            "cdi": properties.get("cdi"),
            "mmi": properties.get("mmi"),
            "alert": properties.get("alert"),
            "status": properties.get("status"),
            "tsunami": properties.get("tsunami"),
            "sig": properties.get("sig"),
            "net": properties.get("net"),
            "code": properties.get("code"),
            "ids": properties.get("ids"),
            "sources": properties.get("sources"),
            "types": properties.get("types"),
            "nst": properties.get("nst"),
            "dmin": properties.get("dmin"),
            "rms": properties.get("rms"),
            "gap": properties.get("gap"),
            "magType": properties.get("magType"),
            "type": properties.get("type"),
            "title": properties.get("title"),
            "area": area_loc,
            "latitude": properties.get("latitude"),
            "longitude": properties.get("longitude"),
            "depth": properties.get("depth"),
            "insert_date":datetime.datetime.now()
        }


# Get a timestamp for the output location
cur_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
landing_location = f'gs://earthquake_analysis_buck/dataflow/landing/{cur_timestamp}/full_earthquake_data.json'
read_loc= landing_location+'*.json'
output_db = 'spark-learning-431506.earthquake_db.dataflow_earthquake_data'
silver_gcs_loction = f'gs://earthquake_analysis_buck/dataflow/silver/{cur_timestamp}/flatten_earthquake_data'
api_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"


# Pipeline options for Dataflow
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=r"D:\Mohini Data Science\GCP\Python_gcp\spark-learning-431506-160e4ffdff74_key.json"
option_obj = PipelineOptions()
google_cloud = option_obj.view_as(GoogleCloudOptions)
google_cloud.project = 'spark-learning-431506'
google_cloud.job_name = "earthquake-ingestion-data"
google_cloud.region = "us-central1"
google_cloud.staging_location = "gs://dataflow_bk123/stagging_loc"
google_cloud.temp_location = "gs://dataflow_bk123/temp_loc"

# Define and run the pipeline
with beam.Pipeline(options=option_obj) as pipeline:
    (
            pipeline
            | "Start Pipeline" >> beam.Create([None])
            | "Fetch Full Earthquake Data" >> beam.ParDo(ExtractDataFormAPI(),api)
            | "Write Full JSON to GCS" >> beam.io.WriteToText(landing_location, shard_name_template="",
                                                              file_name_suffix=".json")
            # |'print location' >> beam.Map(print)
    )


# Run the pipeline
with beam.Pipeline(options=option_obj) as p2:
    trans_data=(
            p2
            | "Read from GCS" >> beam.io.ReadFromText(read_loc)
            | "Extract Required Fields" >> beam.ParDo(ExtractRequiredDataAndFlatten())
            | 'transaforamtion ' >> beam.ParDo(AppyTransformation())
    )

    write_silver_gcs=(trans_data |'clean data write in gcs silver layer' >> beam.io.WriteToText
                                                    (silver_gcs_loction,file_name_suffix='.json',shard_name_template='')
                      )

    write_bq=(trans_data| "Write to BigQuery" >> beam.io.WriteToBigQuery(
                output_db,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )
              )

