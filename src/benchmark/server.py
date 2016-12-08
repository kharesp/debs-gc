import zmq,common,json
from query_processor import Processor

context=zmq.Context()
socket=context.socket(zmq.PULL)
socket.set_hwm(common.hwm)
socket.bind('tcp://*:%d'%6666)

processor=Processor()

while True:
  req=json.loads(socket.recv_string())
  print(req)
  processor.process(req)
