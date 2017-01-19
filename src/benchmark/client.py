import common,json,zmq,time,random,os,math
import threading,datetime,argparse


def send_request(socket,query_type,granularity,\
  query_level,query_target,prediction_win_ts,history,use_initial_model):
  msg={'query_type': query_type.name,\
    'granularity': granularity.name,\
    'query_level': query_level.name,\
    'query_target': query_target,\
    'prediction_win_ts':prediction_win_ts,\
    'history': history.name,\
    'use_initial_model': use_initial_model}


  socket.send_string(json.dumps(msg))
  message=socket.recv_string()
  return message

def micro_benchmark():
  context=zmq.Context()
  socket=context.socket(zmq.REQ)
  socket.connect('tcp://localhost:6666')
  for prediction_window in common.Granularity:
    rand_idx=random.randint(0,common.no_valid_query_readings)
    prediction_win_ts= common.start_query_ts + rand_idx*common.step_size
  
    send_request(socket,common.QueryType.load,\
      prediction_window,\
      common.QueryLevel.house,\
      '0',
      prediction_win_ts,
      common.History.past_5days,False) 
    
    time.sleep(5)

def load(server_locator,num_queries,log_dir):
  expected_interval=2
  context=zmq.Context()
  socket=context.socket(zmq.REQ)
  socket.connect('tcp://%s'%server_locator)

  tid=threading.current_thread().name
  with open('%s/client_%s.csv'%(log_dir,tid),'w') as f:
    start_ts=time.time()
    start_hh_mm_ss=datetime.datetime.fromtimestamp(start_ts).strftime('%H:%M:%S')
    f.write('%s\n'%start_hh_mm_ss)
    #granularities=[0,2,3,6,12]
    houses=range(0,10)
    #households=[0,1]
    #plug=0 
    for i in range(num_queries):
      #rand_query_level=random.randint(0,len(common.QueryLevel)-1)
      #rand_history_level=random.randint(0,len(common.History)-1)
      #rand_granularity=granularities[random.randint(0,len(granularities)-1)]
      rand_query_level=2
      rand_history_level=2
      rand_granularity=12
      rand_idx=random.randint(0,common.no_valid_query_readings)
      prediction_win_ts= common.start_query_ts + rand_idx*common.step_size

      rand_house=houses[random.randint(0,len(houses)-1)]
      #rand_hh=households[random.randint(0,len(households)-1)]
      target=None
      if(common.QueryLevel(rand_query_level)==common.QueryLevel.plug):
        #target='%d_%d_%d'%(plug,rand_hh,rand_house)
        target='11_0_0'
      elif(common.QueryLevel(rand_query_level)==common.QueryLevel.household):
        target='%d_%d'%(rand_hh,rand_house)
      elif(common.QueryLevel(rand_query_level)==common.QueryLevel.house):
        target='%d'%(rand_house)
      else:
        print('invalid')

      msg=send_request(socket,common.QueryType.load,\
        common.Granularity(rand_granularity),\
        common.QueryLevel(rand_query_level),\
        target,\
        prediction_win_ts,\
        common.History(rand_history_level),False)

      f.write('%s\n'%msg)
      
      sleep_interval= -expected_interval*math.log(1-random.random())
      time.sleep(sleep_interval)

    end_ts=time.time()
    end_hh_mm_ss=datetime.datetime.fromtimestamp(end_ts).strftime('%H:%M:%S')
    f.write('%s\n'%end_hh_mm_ss)

def main(server_locator,num_clients,num_queries,log_dir):
  for i in range(num_clients):
    thread=threading.Thread(target=load,\
      args=(server_locator,num_queries,log_dir,))
    thread.start()

if __name__=="__main__":
  parser= argparse.ArgumentParser(description='Client process for sending requests')
  parser.add_argument('server_locator', help=\
    'server address:port')
  parser.add_argument('num_clients',type=int, help=\
    'number of client threads to spawn')
  parser.add_argument('num_requests',type=int, help=\
    'number of requests each thread will send')
  parser.add_argument('run_id', help=\
    'run id to identify test run')
  args=parser.parse_args()
  
  log_dir='log/client/%s'%args.run_id
  if not os.path.exists(log_dir):
    os.makedirs(log_dir) 

  main(args.server_locator,args.num_clients,args.num_requests,log_dir)
