import parse,time_trigger
from rx import Observable
from rx.subjects import Subject
from plug_model import PlugModel

house_stream= Subject()
connectable=parse.lines('data/0_0.csv').multicast(house_stream)
triggers=time_trigger.trigger(house_stream)

def plug_processor(hh_id,plug_stream):
  plug_model=PlugModel(hh_id,plug_stream.key) 
  plug_stream.\
    combine_latest(triggers, lambda r,last_ts: plug_model.process(r,last_ts)).\
    subscribe()
  
def hh_processor(hh_id,hh_stream):
  hh_stream.\
    group_by(lambda r: r.plug_id).\
    subscribe(lambda plug_stream: plug_processor(hh_id,plug_stream))
  

house_stream.\
  filter(lambda r: r.plug_id==7).\
  group_by(lambda r: r.hh_id).\
  subscribe(lambda hh_stream: hh_processor(hh_stream.key,hh_stream))

connectable.connect()
