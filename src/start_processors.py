import argparse,common, multiprocessing
from house import Processor


if __name__=="__main__":

  #parse command line arguments
  parser= argparse.ArgumentParser(description='Starts house processors')
  parser.add_argument('num_processors',type=int, \
    help='number of house processors to start')
  args=parser.parse_args()
 
  port_number= common.starting_processor_port_num
  for i in range(args.num_processors):
    connector_string='tcp://*:%d'%(port_number)
    port_number+=1
    Processor(connector_string).start()
