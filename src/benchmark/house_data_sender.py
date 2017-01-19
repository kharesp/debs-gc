import os,argparse,common,threading
from hh_data_sender import Sender

def send_house_data(h_id,server_address,zmq_port):
  households= [d for d in os.listdir('%s/%s'%(common.datadir,h_id)) if \
    os.path.isdir(os.path.join('%s/%s'%(common.datadir,h_id),d))]

  threads=[threading.Thread(target=Sender(h_id=h_id,hh_id=int(hh_id),\
    server_address=server_address,zmq_port=zmq_port).run) \
    for hh_id in households]

  for t in threads:
    t.start()



if __name__=="__main__":
  parser= argparse.ArgumentParser(description='House data sender')
  parser.add_argument('h_id',type=int, help='h_id of house for which data will be sent') 
  parser.add_argument('server_address',help='server address')
  parser.add_argument('zmq_port',type=int, help='zmq port on which to send data to server') 
  args=parser.parse_args()

  send_house_data(h_id=args.h_id,server_address= \
    args.server_address,zmq_port=args.zmq_port)
