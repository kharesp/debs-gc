from common import Reading
from parse import lines

def timesteps(datafile,outfile,step_size):
  with open(outfile,'w') as f:
    last_ts=-1
    def on_next(r):
      nonlocal last_ts
      if (last_ts==-1):
        last_ts=r.ts  if (r.ts%step_size==0) else (r.ts-(r.ts % step_size))
        f.write('%d\n'%last_ts)

      if ((r.ts - last_ts) >= step_size):
        last_ts=r.ts if (r.ts%step_size==0) else (r.ts-(r.ts % step_size))
        f.write('%d\n'%last_ts)

    def on_completed():
      f.close()
      
    lines(datafile).\
      filter(lambda r: r.property==1).\
      subscribe(on_next=on_next,on_error=None,on_completed=on_completed)
  
if __name__=="__main__":
  timesteps('data/2weeks/sorted.csv','data/2weeks/time_steps.csv',300)
