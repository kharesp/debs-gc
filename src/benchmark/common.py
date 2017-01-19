from enum import Enum

hwm=1000000
datadir='/home/kharesp/workspace/python/debs-gc/data/2weeks/curated'
step_size=300
time_steps=[1377986400,1379196000]
window_size=300000

number_of_steps_in_a_day=287
one_week_start_ts=1377986400
one_week_end_ts=1378598100
start_query_ts=1379030400
no_valid_query_readings=552

class QueryLevel(Enum):
  plug=0
  household=1
  house=2

class Granularity(Enum):
  win_5mins=0
  win_10mins=2
  win_15mins=3
  win_30mins=6
  win_60mins=12

class QueryType(Enum):
  load=0
  outlier=1

class History(Enum):
  past_1day=0
  past_3days=1
  past_5days=2
