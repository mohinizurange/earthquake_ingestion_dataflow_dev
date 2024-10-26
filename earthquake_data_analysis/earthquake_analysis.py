import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions,GoogleCloudOptions
import os

class CuntNumEqa(beam.DoFn):
    def process(self, eq_dic):
        # print(eq_dic,type(eq_dic))
        region = eq_dic['area']

        yield (region,1)

##2.
class FilteringData(beam.DoFn):
    def process(self, eq_dic):
        # print(eq_dic,type(eq_dic))
        region = eq_dic['area']
        mag = eq_dic['mag']
        yield (region, mag)

class RegionWiseAvgMag(beam.DoFn):
    def process(self, ele):
        e_length = len(ele[1])
        e_sum = sum(ele[1])
        e_avg = e_sum/e_length
        region = ele[0]
        yield (region, e_avg)


if __name__ == '__main__':
    os.environ[
        'GOOGLE_APPLICATION_CREDENTIALS'] = r"D:\Mohini Data Science\GCP\Python_gcp\spark-learning-431506-160e4ffdff74_key.json"
    option_obj = PipelineOptions()
    google_cloud = option_obj.view_as(GoogleCloudOptions)
    google_cloud.project = 'spark-learning-431506'
    google_cloud.job_name = "earthquake-analysis-data"
    google_cloud.region = "us-central1"
    google_cloud.staging_location = "gs://dataflow_bk123/stagging_loc"
    google_cloud.temp_location = "gs://dataflow_bk123/temp_loc"

    ## src path
    table_name_src = 'spark-learning-431506.earthquake_db.dataflow_earthquake_data'

    with beam.Pipeline(options=option_obj) as p:
        read_data = (p|'read data from bigquery'>> beam.io.ReadFromBigQuery(table = table_name_src)
                # |'print'>> beam.Map(print)
                )

        # 1. Count the number of earthquakes by region
        num_earthquake_by_region = (read_data|'Count the number of earthquakes'>> beam.ParDo(CuntNumEqa())
                          |'combine per key' >> beam.CombinePerKey(sum)
                          # | 'print' >> beam.Map(print)
                          )


        # 2. Find the average magnitude by the region

        avg_earthquake_by_region = (read_data | 'call class FilteringData' >> beam.ParDo(FilteringData())
                                    | 'group by key' >> beam.GroupByKey() #('Saanichton, Canada', [3.96,10.5])
                                    |'call class RegionWiseAvgMag' >> beam.ParDo(RegionWiseAvgMag())
                                    | 'print' >> beam.Map(print)
                                    )



# 3. Find how many earthquakes happen on the same day.
# 4. Find how many earthquakes happen on same day and in same region
# 5. Find average earthquakes happen on the same day.
# 6. Find average earthquakes happen on same day and in same region
# 7. Find the region name, which had the highest magnitude earthquake last week.
# 8. Find the region name, which is having magnitudes higher than 5.
# 9. Find out the regions which are having the highest frequency and intensity of
# earthquakes.
