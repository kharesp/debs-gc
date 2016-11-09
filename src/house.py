import parse, subscriber, threading, argparse,time,os
from rx import Observable
from rx.subjects import Subject
from plug_model import PlugModel
from common import WinSizes,Update,perf_metrics,house_forecasts
from rx.concurrency import eventloopscheduler
from monitoring import Perf
  
class Processor(threading.Thread):
  
  def __init__(self,address):
    threading.Thread.__init__(self)
    self.address=address
 
  def plug_processor(self,h_id,hh_id,plug_stream):
    plug_model=PlugModel(h_id,hh_id,plug_stream.key) 
    plug_stream.\
      subscribe(lambda u: plug_model.process_update(u))
    
  def hh_processor(self,h_id,hh_stream):
    hh_id=hh_stream.key
    hh_stream.\
      group_by(lambda u: u.reading.plug_id).\
      subscribe(lambda plug_stream: self.plug_processor(h_id,hh_id,plug_stream))
    
  def h_processor(self,h_stream):
    if not os.path.exists('out'):
      os.makedirs('out')

    h_id=h_stream.key
    perf_metrics[h_id]=Perf(h_id)
    house_forecasts[h_id]=open('out/%d.csv'%h_id,'w')
    
    last_ts=-1
    def progress_time(t):
      nonlocal last_ts
      modulo= t['reading'].ts % WinSizes.win_30s.value
      if (last_ts==-1):
        last_ts=t['reading'].ts - (WinSizes.win_30s.value if modulo==0 else modulo)
    
      elapsed_time=t['reading'].ts - last_ts
      if (elapsed_time > WinSizes.win_30s.value):
        last_ts = t['reading'].ts - (WinSizes.win_30s.value if modulo==0 else modulo)
      
      return Update(last_ts=last_ts,reading=t['reading'],reception_ts=t['reception_ts']) 
  
    h_stream. \
      map(progress_time). \
      group_by(lambda u: u.reading.hh_id).\
      subscribe(lambda hh_stream: self.hh_processor(h_id,hh_stream))

  def run(self):
    subscriber.data_stream(self.address).\
      map(lambda r: {'reading':r,'reception_ts':time.perf_counter()}). \
      group_by(lambda t: t['reading'].h_id). \
      subscribe(lambda h_stream: self.h_processor(h_stream))
