import common,time,zmq

class Perf(object):
  def __init__(self,id):
    self.window_start_ts=time.perf_counter()
    self.win_processing_time=0.0
    self.count=0
    self.id=id
    self.stats_file=open('perf/%d.csv'%self.id,'w')
    self.zmq_context=zmq.Context()
    self.sender_socket=self.zmq_context.socket(zmq.PUSH)
    self.sender_socket.set_hwm(common.hwm)
    self.sender_socket.connect('tcp://127.0.0.1:%d'% \
      (common.monitoring_port_num))

  def record(self,reception_ts):
    self.count+=1
    processing_end_ts=time.perf_counter()
    self.win_processing_time+=(processing_end_ts - reception_ts)
    if(self.count % common.perf_window_size == 0):
      ts= time.time()
      latency=self.win_processing_time
      win_thput=(processing_end_ts-self.window_start_ts)
      self.window_start_ts= processing_end_ts 
      self.win_processing_time=0.0
      self.sender_socket.send_pyobj(common.Stats(ts=ts,h_id=self.id,\
        latency=latency,throughput=win_thput))
      self.stats_file.write('%d,%d,%f,%f\n'%(ts,self.id,latency,win_thput))
      self.stats_file.flush()
