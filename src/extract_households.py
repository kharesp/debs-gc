from common import Reading
from parse import lines
import os

def extract_households_from_house(h_id,datadir,outdir):
  if not os.path.exists('%s/%d'%(outdir,h_id)):
    os.makedirs('%s/%d'%(outdir,h_id))

  def write_stream(hh_stream):
    outfile='%s/%d/%d_%d.csv'%(outdir,h_id,hh_stream.key,h_id)
    f=open(outfile,'w')
    def on_next(r):
      f.write('%d,%d,%f,%d,%d,%d,%d\n'%\
        (r.id,r.ts,r.value,r.property,r.plug_id,r.hh_id,r.h_id))
    def on_completed():
      f.close()
      
    hh_stream.subscribe(on_next=on_next,on_error=None,on_completed=on_completed)
   
  lines('%s/%d.csv'%(datadir,h_id)).\
    group_by(lambda r: r.hh_id).\
    subscribe(write_stream)


if __name__=="__main__":
  datadir='data/2weeks'
  outdir='data/2weeks/households'
  for i in range(3):
    extract_households_from_house(i,datadir,outdir)
