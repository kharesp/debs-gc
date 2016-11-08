import argparse,common
from house import Processor


if __name__=="__main__":

  #parse command line arguments
  parser= argparse.ArgumentParser(description='Starts house processors')
  parser.add_argument('server_address',type=str, \
    help='server address')
  parser.add_argument('num_processors',type=int, \
    help='number of house processors to start')
  args=parser.parse_args()
 
  port_number= 5000
  for i in range(args.num_processors):
    connector_string='tcp://%s:%d'%(args.server_address,port_number)
    port_number+=1
    print(connector_string)
    Processor(connector_string).start()
