from subscriber import data_stream
import argparse,common,rx,time,os

if __name__=="__main__":

  if not os.path.exists('perf'):
    os.makedirs('perf')

  with open('perf/metrics.csv','w') as f:
    parser= argparse.ArgumentParser(description='Metrics Receiver')
    parser.add_argument('num_houses',type=int,help='number of houses')
    args=parser.parse_args()
  
    last_readings={i:common.Stats(ts=-1,h_id=-1,latency=0.0,throughput=0.0) \
      for i in range(args.num_houses)}
    
    def process(update):
      print(update.h_id)
      sum_latency=0.0
      sum_throughput=0.0
      count=0 
      last_readings[update.h_id]=update
      for h_id,stats in last_readings.items():
        if((update.ts - stats.ts) < 60):
          sum_latency+= update.latency
          sum_throughput+= update.throughput
          count+=1
      f.write('%d,%f,%f\n'%(time.time(),(sum_latency/count),(sum_throughput/count)))
      f.flush()
       
    data_stream('tcp://127.0.0.1:%d'%common.monitoring_port_num).subscribe(process)
