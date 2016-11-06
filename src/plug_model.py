class PlugModel(object):
  def __init__(self,hh_id,plug_id):
    self.hh_id=hh_id
    self.plug_id=plug_id
  
  def process(self,reading,last_ts):
    flag=False
    if(reading.ts < last_ts):
      flag=True
    
    print('plug_id:%d  reading_ts:%d last_ts:%d flag:%d'%\
        (reading.plug_id,reading.ts,last_ts,flag))

