import parse,os,matplotlib

def plot_hh_load_vs_time_per_house(house_id,out_dir,data_dir):
  print('Processing house_id:%d'%house_id)
  with open('%s/%d.csv'%(out_dir,house_id),'w') as f:
    last_ts=-1
    hh_load_map={}
    def on_next(r):
      nonlocal last_ts,hh_load_map
      if (last_ts==-1):
        last_ts=r.ts
      if(last_ts==r.ts):
        hh_load_map[r.hh_id]= hh_load_map.get(r.hh_id,0.0)+r.value
      else:
        print_results()
        for key in hh_load_map.keys():
          hh_load_map[key]=0.0
        last_ts=r.ts
        hh_load_map[r.hh_id]=r.value
        

    def on_completed():
      print_results()
      f.close()
    
    def print_results():
      nonlocal last_ts,hh_load_map
      max_hh_id=max(hh_load_map.keys())
      output_str='%d,'%last_ts
      for i in range(max_hh_id+1):
        output_str+='%f,'%hh_load_map.get(i,0.0)
      f.write(output_str.rstrip(',')+'\n')
   
    parse.lines('%s/%d.csv'%(data_dir,house_id)).\
      subscribe(on_next=on_next,on_error=None,on_completed=on_completed)

if __name__=="__main__":
  data_dir='data'
  #ensure output folder exists
  out_dir='graphs'
  if not os.path.exists(out_dir):
    os.makedirs(out_dir)  

  for i in range(40):
    plot_hh_load_vs_time_per_house(i,out_dir,data_dir)
