import os,glob,time
from common import Reading
from parse import lines,read

def write_wday_step_size_index(filename,step_size,time_steps):
  with open(filename,'w') as f:
    f.write('weekday,')
    for step in range(time_steps[0],time_steps[1]+step_size,step_size):
      t_struct=time.gmtime(step)
      f.write('%d,'%t_struct.tm_wday)
    f.write('\n')
    f.write('index,')
    for step in range(time_steps[0],time_steps[1]+step_size,step_size):
      t_struct=time.gmtime(step)
      index= (t_struct.tm_hour*60*60+t_struct.tm_min*60+t_struct.tm_sec)//step_size
      f.write('%d,'%index)
    f.write('\n')

def get_wday(step_size,time_steps):
  for step in range(time_steps[0],time_steps[1]+step_size,step_size):
    t_struct=time.gmtime(step)
    print('ts:%d \t wday:%d\n'%(step,t_struct.tm_wday))

def get_step_size_index(step_size,time_steps):
  for step in range(time_steps[0],time_steps[1]+step_size,step_size):
    t_struct=time.gmtime(step)
    index= (t_struct.tm_hour*60*60+t_struct.tm_min*60+t_struct.tm_sec)//step_size
    print('ts:%d \t index:%d\n'%(step,index))
  
def collate_results(datadir,step_size,time_steps):
  houses=os.listdir(datadir)
  for house in houses:
    f=open('%s/%s/summary_%s.csv'%(datadir,house,house),'w')
    #write all time steps
    f.write('time steps,')
    for step in range(time_steps[0],time_steps[1]+step_size,step_size):
      f.write('%d,'%step)
    f.write('\n')
    
    #write house load
    house_summary=open('%s/%s/%s.csv'%(datadir,house,house),'r')
    f.write('house_%s,'%(house))
    for line in house_summary:
      f.write('%f,'%(float(line.rstrip().split(',')[1])))
    f.write('\n')
    house_summary.close()

    #write household load
    households=[d for d in os.listdir('%s/%s'%(datadir,house)) if \
      os.path.isdir(os.path.join('%s/%s'%(datadir,house),d))]
    for hh in households:
      f.write('household_%s_%s,'%(hh,house))
      hh_file=open('%s/%s/%s/%s_%s.csv'%(datadir,house,hh,hh,house),'r')
      for line in hh_file:
        f.write('%f,'%(float(line.rstrip().split(',')[1])))
      f.write('\n')
      hh_file.close()
 
      #write plug loads within this household
      curated_plugs=glob.glob('%s/%s/%s/curated_*.csv'%(datadir,house,hh))
      for plug_file in curated_plugs:
        f.write('plug_%s,'%(plug_file.partition('_')[2].partition('.')[0]))
        plug=open(plug_file,'r')
        for line in plug:
          f.write('%f,'%(float(line.rstrip().split(',')[1])))
        f.write('\n')
        plug.close()
    
    f.close()
  
  
def sum_hh_in_house(datadir,step_size,time_steps):
  houses=os.listdir(datadir)
  for house in houses:
    households=os.listdir('%s/%s'%(datadir,house))
    f=open('%s/%s/%s.csv'%(datadir,house,house),'w')
    hh_files=[open('%s/%s/%s/%s_%s.csv'%(datadir,house,hh,hh,house),'r') for hh in households]
    for time_step in range(time_steps[0],time_steps[1]+step_size,step_size):
      sum=0.0
      for hh in hh_files:
        reading=float(hh.readline().rstrip().split(',')[1])
        sum+=reading
      f.write('%d,%f\n'%(time_step,sum))
    f.close()
    (hh.close() for hh in hh_files)

def sum_plugs_in_hh(datadir,step_size,time_steps):
  houses=os.listdir(datadir)
  for house in houses:
    households=os.listdir('%s/%s'%(datadir,house))
    for hh in households:
      f=open('%s/%s/%s/%s_%s.csv'%(datadir,house,hh,hh,house),'w')
      curated_plugs=glob.glob('%s/%s/%s/curated_*.csv'%(datadir,house,hh))
      plugs= [open(plug_file,'r') for plug_file in curated_plugs]
      for time_step in range(time_steps[0],time_steps[1]+step_size,step_size):
        sum=0.0
        for plug in plugs:
          reading=float(plug.readline().rstrip().split(',')[1])
          sum+=reading
        f.write('%d,%f\n'%(time_step,sum))
      f.close()
      (plug.close() for plug in plugs)

def curate_plugs(datadir,step_size,time_steps):
  houses=os.listdir(datadir)
  for house in houses:
    households=os.listdir('%s/%s'%(datadir,house))
    for hh in households:
      processed_plugs=glob.glob('%s/%s/%s/processed_*.csv'%(datadir,house,hh))
      for plug in processed_plugs:
        out_file='%s/%s/%s/curated_%s'%(datadir,house,hh,plug.partition('_')[2])
        f=open(out_file,'w')
        plug_vals=open(plug,'r')
        i=time_steps[0]
        processed_plug_ts=-1
        processed_plug_val=-1
        while (i<=time_steps[1]):
          line=plug_vals.readline().rstrip()
          if (not line==''):
            lst=line.split(',')
            processed_plug_ts=int(lst[0])
            processed_plug_val=float(lst[1])
          while (i<processed_plug_ts):
            f.write('%d,%f\n'%(i,0.0))
            i=i+step_size
            
          if(i==processed_plug_ts):
            f.write('%d,%f\n'%(i,processed_plug_val))

          if(i>processed_plug_ts):
            f.write('%d,%f\n'%(i,0.0))
          i+=step_size
        plug_vals.close()
        f.close()
            

def process_plugs(datadir,time_step):
  houses=os.listdir(datadir)
  for house in houses:
    households=os.listdir('%s/%s'%(datadir,house))
    for hh in households:
      plugs=os.listdir('%s/%s/%s'%(datadir,house,hh))
      for plug in plugs:
        print('Processing house:%s hh:%s plug:%s'%(house,hh,plug))
        plug_file='%s/%s/%s/%s'%(datadir,house,hh,plug)
        out_file='%s/%s/%s/processed_%s'%(datadir,house,hh,plug)
        f=open(out_file,'w')
        last_ts=-1
        written_ts=-1
        sum=0.0
        count=0
        def on_next(r):
          nonlocal last_ts,written_ts,sum,count
          if (last_ts==-1):
            last_ts=r.ts if (r.ts%time_step==0) else (r.ts-(r.ts%time_step))
          if ((r.ts-last_ts) < time_step):
            sum+=r.value
            count+=1
          else:
            avg= (sum/count) if (count>0) else 0.0
            f.write('%d,%f\n'%(last_ts,avg))
            written_ts=last_ts
            last_ts=r.ts if (r.ts%time_step==0) else (r.ts-(r.ts%time_step))
            sum=r.value
            count=1

        def on_completed():
          if (last_ts > written_ts):
            avg= (sum/count) if (count>0) else 0.0
            f.write('%d,%f\n'%(last_ts,avg))
          f.close()
          
        lines(plug_file).subscribe(on_next=on_next,\
          on_error=None,on_completed=on_completed)

def remove_processed_files(datadir):
  houses=os.listdir(datadir)
  for house in houses:
    households=os.listdir('%s/%s'%(datadir,house))
    for hh in households:
      processed_plugs=glob.glob('%s/%s/%s/processed*.csv'%(datadir,house,hh))
      for plug in processed_plugs:
        os.remove(plug)

def remove_curated_files(datadir):
  houses=os.listdir(datadir)
  for house in houses:
    households=os.listdir('%s/%s'%(datadir,house))
    for hh in households:
      curated_plugs=glob.glob('%s/%s/%s/curated*.csv'%(datadir,house,hh))
      for plug in curated_plugs:
        os.remove(plug)

def remove_hh_files(datadir):
  houses=os.listdir(datadir)
  for house in houses:
    households=os.listdir('%s/%s'%(datadir,house))
    for hh in households:
      os.remove('%s/%s/%s/%s_%s.csv'%(datadir,house,hh,hh,house))

def remove_h_files(datadir):
  houses=os.listdir(datadir)
  for house in houses:
    os.remove('%s/%s/%s.csv'%(datadir,house,house))

if __name__ == "__main__":
  datadir='data/2weeks/plugs'
  step_size=300
  time_steps=[1377986400,1379196000]
  #process_plugs(datadir,step_size)
  #curate_plugs(datadir,300,time_steps)
  #sum_plugs_in_hh(datadir,step_size,time_steps)
  #sum_hh_in_house(datadir,step_size,time_steps)
  #collate_results(datadir,step_size,time_steps)
  filename='/home/kharesp/workspace/python/debs-gc/data/2weeks/plugs/summary/time.csv'
  write_wday_step_size_index(filename,step_size,time_steps)
