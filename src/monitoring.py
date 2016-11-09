import common,time,zmq

class Perf(object):
  def __init__(self,id):
    self.start_ts=time.perf_counter()
    self.last_ts=-1
    self.count=0
    self.tot_processing_time=0
    self.id=id

    self.zmq_context=zmq.Context()
    self.sender_socket=self.zmq_context.socket(zmq.PUSH)
    self.sender_socket.connect('tcp://127.0.0.1:%d'% \
      (common.monitoring_port_num))

  def record(self,reception_ts):
    self.count+=1
    processing_end_ts=time.perf_counter()
    self.tot_processing_time+=(processing_end_ts - reception_ts)
    if(self.count % 10000 == 0):
      latency=self.tot_processing_time/self.count
      thput=self.count/(processing_end_ts -self.start_ts)
      print('h_id:%d latency:%f thput:%f\n'%(self.id,latency,thput))
      self.sender_socket.send_pyobj(common.Stats(h_id=self.id,\
        latency=latency,throughput=thput))
