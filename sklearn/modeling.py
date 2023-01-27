from google.cloud import bigquery
client = bigquery.Client()


sql = """
SELECT * from `citibike-31437.weather.nyc_weather` WHERE YEAR(timestamp) == '2020' LIMIT 1000
"""

weatherDF = client.query(sql).to_dataframe().fillna(0)
print(weatherDF.head())
