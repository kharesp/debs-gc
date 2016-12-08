from enum import Enum

hwm=1000000
datadir='data/2weeks/plugs'
step_size=300
time_steps=[1377986400,1378286400]
window_size=300000

class QueryLevel(Enum):
  plug=0
  household=1
  house=2

class Granularity(Enum):
  win_5mins=0
  win_10mins=2
  win_15mins=3
  win_30mins=6

class QueryType(Enum):
  load=0
  outlier=1
