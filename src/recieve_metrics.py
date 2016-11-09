from subscriber import data_stream
import argparse,common,rx,time


if __name__=="__main__":
  with open('perf/metrics.csv','w') as f:
    parser= argparse.ArgumentParser(description='Metrics Receiver')
    parser.add_argument('num_houses',type=int,help='number of houses')
    args=parser.parse_args()

    data_stream('tcp://127.0.0.1:%d'%common.monitoring_port_num). \
      subscribe(lambda r: print('%f'%r.h_id))
