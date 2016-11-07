from common import State, WinSizes 

class PlugModel(object):
  def __init__(self,hh_id,plug_id):
    self.hh_id=hh_id
    self.plug_id=plug_id
    self.last_ts=-1
    self.last_id=-1
    self.update_window= State(last_ts= -1,
      load=0.0,
      count=0)
    self.time_windows= { size.name: State(last_ts=-1, load=0.0, count=0) \
      for size in WinSizes}
    self.initialized=False
    
  
  def process(self,reading,last_ts):
    # Initialization
    ts_update=False
    reading_update=False

    if (not self.initialized):
      self.last_ts=last_ts    
      self.last_id=reading.id
      self.initialized= True
    
    #identify if it is a ts update or a reading update
    if (self.last_ts != last_ts):#ts update
      self.last_ts=last_ts
      ts_update=True

    if (self.last_id != reading.id): #reading update
      self.last_id=reading.id
      reading_update=True

    #if(ts_update):
    #  print('ts')

    #if(reading_update):
    #  print('reading')
    print('reading ts:%d \t last_ts:%d'%(reading.ts,last_ts))
