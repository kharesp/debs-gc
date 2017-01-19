import zmq,common,json,time,threading,argparse,os,datetime
from time import strftime, gmtime
from median_processor import Processor

def worker_routine(url_workers,log_dir):
  tid=threading.current_thread().name
  context=zmq.Context.instance()
  socket= context.socket(zmq.REP)
  socket.connect(url_workers)
  processor=Processor()

  with open('%s/worker_%s.csv'%(log_dir,tid),'w') as f:
    start_ts=time.time()
    start_hh_mm_ss=datetime.datetime.fromtimestamp(start_ts).strftime('%H:%M:%S')
    f.write("time:%s\tts:%d\tWorker:%s started\n"%\
      (start_hh_mm_ss,start_ts,tid))

    while True:
      req=json.loads(socket.recv_string())
      reception_ts=time.time()
      reception_hh_mm_ss=datetime.datetime.fromtimestamp(reception_ts).strftime('%H:%M:%S')

      f.write("time:%s\tts:%d\tWorker:%s received request\n"%\
        (reception_hh_mm_ss,reception_ts,tid))
      f.flush()

      result=processor.process(req)
     
      response_ts=time.time() 
      response_hh_mm_ss=datetime.datetime.fromtimestamp(response_ts).strftime('%H:%M:%S')
      f.write("time:%s\tts:%d\tWorker:%s received response\n"%\
        (response_hh_mm_ss,response_ts,tid))
      f.flush()
      socket.send_string('%f'%(result['elapsed_time']))

def main(socket,num_workers,log_dir):
  url_workers="inproc://workers"
  url_clients="tcp://*:%d"%socket

  context=zmq.Context.instance()
  client_socket= context.socket(zmq.ROUTER)
  client_socket.bind(url_clients)

  worker_socket= context.socket(zmq.DEALER)
  worker_socket.bind(url_workers)
 
  for i in range(num_workers):
    thread= threading.Thread(target=worker_routine,\
      args=(url_workers,log_dir,))
    thread.start()
  zmq.proxy(client_socket,worker_socket)

if __name__=="__main__":
  parser= argparse.ArgumentParser(description='Server for handling client queries')
  parser.add_argument('zmq_port',type=int, help=\
    'zmq port at which server will listen for client requests')
  parser.add_argument('threadpool_size',type=int, help=\
    'size of threadpool to service client requests')
  parser.add_argument('run_id', help=\
    'run id to identify this test')
  args=parser.parse_args()

  print("Starting Server with zmq_port:%d and threadpool size:%d\n"%\
    (args.zmq_port,args.threadpool_size))
  
  log_dir='log/server/%s'%args.run_id
  if not os.path.exists(log_dir):
    os.makedirs(log_dir) 

  main(args.zmq_port,args.threadpool_size,log_dir)
