import pandas as pd
from datetime import datetime

site_timetables = pd.read_csv('site_timetables.csv', parse_dates=['opening_datetime_utc', 'closing_datetime_utc'])

site_timetables = site_timetables.dropna()

now = datetime(2023, 4, 3, 14, 0, 0)

opened_sites = site_timetables[(site_timetables['opening_datetime_utc'] <= now) & ((site_timetables['closing_datetime_utc'] > now) | site_timetables['closing_datetime_utc'].isnull())]

opened_sites = pd.DataFrame(opened_sites)

sensors_site = pd.read_csv("sensors_site.csv")

opened_sites_sensors = pd.merge(opened_sites, sensors_site, on='site_id')

records = pd.read_csv('records.csv')

records['record_datetime'] = pd.to_datetime(records['record_datetime'])

newest_records = records.groupby('sensor_id').agg({'record_datetime': 'max', 'entries': 'last', 'exits': 'last'}).reset_index()

newest_records.columns = ['sensor_id', 'newest_record_datetime', 'newest_entries', 'newest_exits']

opened_sites_sensors_last_record = pd.merge(opened_sites_sensors, newest_records, on='sensor_id')

opened_sites_sensors_last_record['time_diff'] = (now - opened_sites_sensors_last_record['newest_record_datetime']).dt.total_seconds() / 60

sites_sensors = opened_sites_sensors_last_record[["sensor_id", "time_diff"]]

df = sites_sensors.loc[sites_sensors["time_diff"]>=120]

def assign_level(time_diff):
    if time_diff < 24*60:
        return 1
    elif time_diff < 48*60:
        return 2
    else:
        return 3

df2 = df.copy()
df2.loc[:, 'level'] = df2['time_diff'].apply(lambda x: assign_level(x))

df_merged = df2.merge(opened_sites_sensors_last_record[['sensor_id', 'newest_record_datetime']], on='sensor_id')

df_grouped = df_merged.groupby('sensor_id')['newest_record_datetime'].max().reset_index()

df_result = df_grouped.merge(records[['sensor_id', 'sensor_name']], on='sensor_id').merge(df2, on='sensor_id')

df_result = df_result.drop_duplicates(subset=['sensor_id', 'time_diff', 'level'])
df_result = df_result[['sensor_id', 'sensor_name', 'time_diff', 'level', 'newest_record_datetime']]

df_result = df_result.merge(sensors_site, on="sensor_id")

df_result["time_diff"] = df_result["time_diff"]/60

df_result = df_result.rename(columns={'sensor_id': 'sensor_id', 'site_id': 'site_id', 'sensor_name': 'SensorName', 'time_diff': 'alert_datetime', 'level': 'alert_level', 'newest_record_datetime': 'last_record_datetime'})


import mysql.connector

# connect to MySQL database
db = mysql.connector.connect(
  host="localhost",
  user="username",
  password="password",
  database="database"
)

# create the sensors_alerts table
cursor = db.cursor()
cursor.execute("CREATE TABLE sensors_alerts (alert_id INT(11) NOT NULL AUTO_INCREMENT, sensor_name VARCHAR(45) NOT NULL, sensor_id VARCHAR(45) NOT NULL, alert_level ENUM('1', '2', '3') NOT NULL, alert_datetime DATETIME NOT NULL, site_id INT(11) NOT NULL, last_record_datetime DATETIME NOT NULL, PRIMARY KEY (alert_id))")

# insert rows into the sensors_alerts table
cursor = db.cursor()
for row in df_result.itertuples(index=False):
    sql = "INSERT INTO sensors_alerts (sensor_id, sensor_name, alert_datetime, alert_level, last_record_datetime, site_id) VALUES (%s, %s, %s, %s, %s, %s)"
    val = (row.sensor_id, row.SensorName, row.alert_datetime, row.alert_level, row.last_record_datetime, row.site_id)
    cursor.execute(sql, val)

# commit changes and close the connection
db.commit()
db.close()
