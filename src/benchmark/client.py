import common,json,zmq

context=zmq.Context()
socket=context.socket(zmq.PUSH)
socket.connect('tcp://localhost:6666')



def send_request(query_type,granularity,\
  query_level,query_target):
  msg={'query_type': query_type.name,\
    'granularity': granularity.name,\
    'query_level': query_level.name,\
    'query_target': query_target}

  global socket
  socket.send_string(json.dumps(msg))

send_request(common.QueryType.load,\
  common.Granularity.win_10mins,\
  common.QueryLevel.plug,\
  '11_0_0') 
