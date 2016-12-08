from common import Reading
from parse import lines
import os

def extract_plugs_from_house(h_id,datadir,outdir):
  def process_plug_stream(hh_id,plug_stream):
    outfile='%s/%d/%d/%d_%d_%d.csv'%(outdir,h_id,hh_id,plug_stream.key,hh_id,h_id)
    f= open(outfile,'w')
    def on_next(r):
      f.write('%d,%d,%f,%d,%d,%d,%d\n'%\
        (r.id,r.ts,r.value,r.property,r.plug_id,r.hh_id,r.h_id))
    def on_completed():
      f.close()

    plug_stream.subscribe(on_next=on_next,\
      on_error=None,on_completed=on_completed)

  def process_hh_stream(hh_stream):
    if not os.path.exists('%s/%d/%d'%(outdir,h_id,hh_stream.key)):
      os.makedirs('%s/%d/%d'%(outdir,h_id,hh_stream.key))
    hh_stream.\
      group_by(lambda r: r.plug_id).\
      subscribe(lambda plug_stream: process_plug_stream(hh_stream.key,plug_stream))

  lines('%s/%d.csv'%(datadir,h_id)).\
    group_by(lambda r: r.hh_id).\
    subscribe(process_hh_stream)


if __name__=="__main__":
  datadir='data/2weeks'
  outdir='data/2weeks/plugs'
  for i in range(4,40):
    print('extracting plugs for house:%d'%i)
    extract_plugs_from_house(i,datadir,outdir)
