from common import Reading
from parse import lines

def extract_houses(datafile,outdir):
  def write_stream(h_stream):
    print("Extracting house stream for hosue:%s\n"%h_stream.key)
    outfile='%s/%d.csv'%(outdir,h_stream.key)
    f=open(outfile,'w')
    def on_next(r):
      f.write('%d,%d,%f,%d,%d,%d,%d\n'%\
        (r.id,r.ts,r.value,r.property,r.plug_id,r.hh_id,r.h_id))
    def on_completed():
      f.close()

    h_stream.subscribe(on_next=on_next,\
      on_error=None,on_completed=on_completed)

  lines(datafile).\
    filter(lambda r: r.property==1).\
    group_by(lambda r: r.h_id).\
    subscribe(write_stream)


if __name__=="__main__":
    extract_houses('data/2weeks/sorted.csv','data/2weeks')
