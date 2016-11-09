from subscriber import data_stream
import argparse,common,rx,time,os

if __name__=="__main__":

  if not os.path.exists('perf'):
    os.makedirs('perf')


  with open('perf/metrics.csv','w') as f:
    parser= argparse.ArgumentParser(description='Metrics Receiver')
    parser.add_argument('win_size',type=int,help=\
      'size of win in seconds for metrics calculation')
    args=parser.parse_args()
  
    win_start_ts=-1
    sum_latency=0.0
    sum_throughput=0.0
    num_readings=0
    
    def process(update):
      global win_start_ts,sum_latency,sum_throughput,num_readings
      if (win_start_ts == -1):
        win_start_ts=update.ts
     
      if ((update.ts - win_start_ts) > args.win_size):  
        latency=(sum_latency)/(num_readings*common.perf_window_size)
        throughput=(num_readings*common.perf_window_size)/(sum_throughput)
        f.write('%d,%f,%f\n'%(win_start_ts,latency,throughput))
        f.flush()
        win_start_ts=update.ts
        num_readings=0
        sum_latency=0.0
        sum_throughput=0.0

      num_readings+=1
      sum_latency+=update.latency
      sum_throughput+=update.throughput  

       
    data_stream('tcp://*:%d'%common.monitoring_port_num).subscribe(process)
