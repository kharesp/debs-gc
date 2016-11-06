from common import time_windows
import parse
from rx import Observable
from rx.subjects import Subject

def trigger(source):
  def time(observer):
    last_ts= -1
    def track(reading):
      nonlocal last_ts
      modulo= reading.ts%time_windows['30s']
      if (last_ts==-1):
        last_ts=reading.ts - (time_windows['30'] if modulo==0 else modulo)
      elapsed_time=reading.ts - last_ts
      if (elapsed_time > time_windows['30s']):
        last_ts = reading.ts - (time_windows['30'] if modulo==0 else modulo)
        observer.on_next(last_ts)
   
    source.subscribe(track)

  return Observable.create(time)

#class Trigger(object):
#  def __init__(self, source):
#    self.source=source
#    self.subject= Subject()
#    self.last_ts=-1
#
#  def subscribe(self,observer):
#    def track(reading):
#      modulo= reading.ts%time_windows['30s']
#      if (self.last_ts==-1):
#        self.last_ts=reading.ts - (time_windows['30'] if modulo==0 else modulo)
#      elapsed_time=reading.ts - self.last_ts
#      if (elapsed_time > time_windows['30s']):
#        self.last_ts = reading.ts - (time_windows['30'] if modulo==0 else modulo)
#        self.subject.on_next(self.last_ts)
#
#    self.subject.subscribe(observer)
#    self.source.subscribe(track)

