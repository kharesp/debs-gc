import zmq,common,time,json,argparse,parse

if __name__=="__main__":
  #parse command line arguments
  parser= argparse.ArgumentParser(description='House data publisher')
  parser.add_argument('server_address',type=str, \
    help='server connection string like tcp://address:port')
  parser.add_argument('house_data_file',type=str,help='house data file path')
  args=parser.parse_args()
  
  context=zmq.Context()
  publisher=context.socket(zmq.PUSH)
  publisher.set_hwm(common.hwm)
  publisher.connect(args.server_address)

  count=0
  with open(args.house_data_file) as f:
    for line in f:
      count+=1
      publisher.send_pyobj(parse.reading(line.split(',')))
      if (count%10000==0):
        time.sleep(1)
      
  publisher.close()
  print('publisher sent %d messages\n'%(count))
