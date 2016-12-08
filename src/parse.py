from rx import Observable
import numpy as np
from common import Reading


def reading(str_values):
  return Reading(id= int(str_values[0]),
    ts= int(str_values[1]),
    value= float(str_values[2]),
    property= int(str_values[3]),
    plug_id= int(str_values[4]),
    hh_id= int(str_values[5]),
    h_id= int(str_values[6]))

def read(filename):
  def read_file(observer):
    with open(filename,'r') as f:
      for line in f:
        observer.on_next(line.rstrip())
      observer.on_completed() 
    
  return Observable.create(read_file)

def lines(filename):
  def read_file(observer):
    with open(filename,'r') as f:
      for line in f:
        observer.on_next(line)
      observer.on_completed() 
    
  return Observable.create(read_file).\
    map(lambda line: reading(line.split(',')))

def extract_plug_hh_h(datafile,plug_id,hh_id,h_id):
  outfile='data/%d_%d_%d.csv'%(plug_id,hh_id,h_id)
  with open(outfile,'w') as f:
    lines(datafile).\
      filter(lambda r: r.plug_id==plug_id and \
        r.hh_id==hh_id and r.h_id==h_id and r.property==1).\
      subscribe(lambda r: f.write('%d,%d,%f,%d,%d,%d,%d\n' %\
        (r.id,r.ts,r.value,r.property,r.plug_id,r.hh_id,r.h_id)))

def extract_hh_h(datafile,hh_id,h_id):
  outfile='data/%d_%d.csv'%(hh_id,h_id)
  with open(outfile,'w') as f:
    lines(datafile).\
      filter(lambda r: r.hh_id==hh_id and r.h_id==h_id and r.property==1).\
      subscribe(lambda r: f.write('%d,%d,%f,%d,%d,%d,%d\n' %\
        (r.id,r.ts,r.value,r.property,r.plug_id,r.hh_id,r.h_id)))

def extract_h(datafile,h_id):
  outfile='data/%d.csv'%(h_id)
  with open(outfile,'w') as f:
    lines(datafile).\
      filter(lambda r: r.h_id==h_id and r.property==1).\
      subscribe(lambda r: f.write('%d,%d,%f,%d,%d,%d,%d\n' %\
        (r.id,r.ts,r.value,r.property,r.plug_id,r.hh_id,r.h_id)))

def extract_subset(filename,duration):
  def read_file(observer):
    with open(filename) as f:
      first_ts=-1
      for line in f:
        r=reading(line.split(','))
        if (first_ts==-1):
          first_ts=r.ts
        if ((r.ts-first_ts) < (duration)):
          observer.on_next(r)
        else:
          observer.on_completed() 
          break
    
  return Observable.create(read_file)

def write_readings_stream(stream,outfile):
  with open(outfile,'w') as f:
    def on_next(r):
      f.write('%d,%d,%f,%d,%d,%d,%d\n' %\
        (r.id,r.ts,r.value,r.property,r.plug_id,r.hh_id,r.h_id))
    def on_completed():
      f.close()

    stream.subscribe(on_next=on_next,on_error=None,on_completed=on_completed)

if __name__=="__main__":
  write_readings_stream(extract_subset('data/full/sorted.csv',14*24*3600),\
    'data/2weeks/sorted.csv')
