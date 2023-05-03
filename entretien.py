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
    if time_diff > 48*60:
        return 3
    elif time_diff > 24*60:
        return 2
    elif time_diff > 120:
        return 1
    else:
        return 0

df['level'] = df['time_diff'].apply(lambda x: assign_level(x))

print(df)
