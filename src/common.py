from collections import namedtuple
from enum import Enum

class WinSizes(Enum):
  win_30s= 30
  win_1m= 60
  win_5m= 300 
  win_15m= 900 
  win_60m= 3600 
  win_120m= 7200 

Reading=namedtuple("Reading","id ts value property plug_id hh_id h_id")
State=namedtuple("State","last_ts load count")
