from enum import Enum

hwm= 1000000
address= 'tcp://127.0.0.1:5000'

reading_fields=['id','ts','value','property','plug_id','hh_id','h_id']
data_file='data/0.csv'

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
