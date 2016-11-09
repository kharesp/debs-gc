from enum import Enum

hwm= 1000000
reading_fields=['id','ts','value','property','plug_id','hh_id','h_id']
perf_metrics_path='perf/'
perf_metrics={}
house_forecasts={}
perf_window_size=1000

starting_processor_port_num= 5000
monitoring_port_num= 6000

class WinSizes(Enum):
  win_30s= 30
  win_1m= 60
  win_5m= 300 
  win_15m= 900 
  win_60m= 3600 
  win_120m= 7200 

class Reading(dict):
  def __init__(self,**kwargs):
    self.__dict__.update(kwargs)

class Update(dict):
  def __init__(self,**kwargs):
    self.__dict__.update(kwargs)

class State(dict):
  def __init__(self,**kwargs):
    self.__dict__.update(kwargs)

class Stats(dict):
  def __init__(self,**kwargs):
    self.__dict__.update(kwargs)

