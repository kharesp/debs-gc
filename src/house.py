import parse, subscriber, threading
from rx import Observable
from rx.subjects import Subject
from plug_model import PlugModel
from common import WinSizes,Update
from rx.concurrency import eventloopscheduler
  
def plug_processor(h_id,hh_id,plug_stream):
  plug_model=PlugModel(h_id,hh_id,plug_stream.key) 
  plug_stream.\
    subscribe(lambda u: plug_model.process_update(u))
  
def hh_processor(h_id,hh_stream):
  hh_id=hh_stream.key
  hh_stream.\
    group_by(lambda u: u.reading.plug_id).\
    subscribe(lambda plug_stream: plug_processor(h_id,hh_id,plug_stream))
  

def h_processor(h_stream):
  h_id=h_stream.key
  last_ts=-1
  def progress_time(reading):
    nonlocal last_ts
    modulo= reading.ts % WinSizes.win_30s.value
    if (last_ts==-1):
      last_ts=reading.ts - (WinSizes.win_30s.value if modulo==0 else modulo)
  
    elapsed_time=reading.ts - last_ts
    if (elapsed_time > WinSizes.win_30s.value):
      last_ts = reading.ts - (WinSizes.win_30s.value if modulo==0 else modulo)
    
    return Update(last_ts=last_ts,reading=reading) 

  h_stream. \
    observe_on(eventloopscheduler.EventLoopScheduler()). \
    map(progress_time). \
    group_by(lambda u: u.reading.hh_id).\
    subscribe(lambda hh_stream: hh_processor(h_id,hh_stream))
    
    
if __name__== "__main__":
  subscriber.data_stream(). \
    group_by(lambda r: r.h_id). \
    subscribe(h_processor)
