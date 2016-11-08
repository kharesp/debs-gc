import parse, subscriber, threading
from rx import Observable
from rx.subjects import Subject
from plug_model import PlugModel
from common import WinSizes,Update
from rx.concurrency import eventloopscheduler, newthreadscheduler,threadpoolscheduler

last_ts=-1
def progress_time(reading):
  global last_ts
  modulo= reading.ts % WinSizes.win_30s.value
  if (last_ts==-1):
    last_ts=reading.ts - (WinSizes.win_30s.value if modulo==0 else modulo)

  elapsed_time=reading.ts - last_ts
  if (elapsed_time > WinSizes.win_30s.value):
    last_ts = reading.ts - (WinSizes.win_30s.value if modulo==0 else modulo)
  
  return Update(last_ts=last_ts,reading=reading) 
  
def plug_processor(hh_id,plug_stream):
  plug_model=PlugModel(hh_id,plug_stream.key) 
  plug_stream.\
    subscribe(lambda u: plug_model.process_update(u))
  
def hh_processor(hh_id,hh_stream):
  hh_stream.\
    group_by(lambda u: u.reading.plug_id).\
    subscribe(lambda plug_stream: plug_processor(hh_id,plug_stream))
  

def test(hh_stream):
  hh_stream. \
    observe_on(newthreadscheduler.Scheduler.new_thread). \
    subscribe(lambda r: \
      print('Observed hh_id:%d on thread id:%s\n' \
        %(r.hh_id,threading.current_thread().name)))

if __name__== "__main__":
  subscriber.data_stream(). \
    group_by(lambda r: r.hh_id). \
    subscribe(test)
    
  #update_stream= Subject()
  #connectable= subscriber.data_stream(). \
  #  map(progress_time).\
  #  multicast(update_stream)
  #
  #update_stream.\
  #  group_by(lambda u: u.reading.hh_id).\
  #  subscribe(lambda hh_stream: hh_processor(hh_stream.key,hh_stream))
  #
  #connectable.connect()
