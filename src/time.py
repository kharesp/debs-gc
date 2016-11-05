from common import time_windows
import parse

def time(observer):
  last_ts= None
  def track(reading):
    print(reading)
    modulo= reading.ts%time_windows['30s']
    if (last_ts!=None):
      last_ts=reading.ts - (time_window['30'] if modulo==0 else modulo)
    elapsed_time=reading.ts - last_ts
    if (elapsed_ts > time_window['30s']):
      last_ts = reading.ts - (time_window['30'] if modulo==0 else modulo)
      observer.on_next(last_ts)
   
  parse.lines('data/7_0_0.csv').subscribe(track)
  

parse.lines('data/7_0_0.csv').subscribe(lambda r: print(repr(r)))
#Observable.create(time).subscribe(print)
