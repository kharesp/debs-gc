import parse,os,matplotlib

def plot_plugs_vs_time_per_house(house_id,out_dir,data_dir):
  print('Processing house_id:%d'%house_id)
  with open('%s/%d.csv'%(out_dir,house_id),'w') as f:
    last_ts=-1
    plug_ids=set()
    def on_next(r):
      nonlocal last_ts,plug_ids
      if (last_ts==-1):
        last_ts=r.ts
      if(last_ts==r.ts):
        plug_ids.add('%d_%d_%d'%(r.plug_id,r.hh_id,r.h_id))
      else:
        f.write('%d,%d\n'%(last_ts,len(plug_ids)))
        last_ts=r.ts
        plug_ids.clear()

    def on_completed():
      f.write('%d,%d\n'%(last_ts,len(plug_ids)))
      f.close()

    parse.lines('%s/%d.csv'%(data_dir,house_id)).\
      filter(lambda r: r.value > 0).\
      subscribe(on_next=on_next,on_error=None,on_completed=on_completed)


if __name__=="__main__":
  data_dir='data'
  #ensure output folder exists
  out_dir='graphs'
  if not os.path.exists(out_dir):
    os.makedirs(out_dir)  

  for i in range(40):
    plot_plugs_vs_time_per_house(i,out_dir,data_dir)
