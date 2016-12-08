import os,glob,zmq,json,time,common,threading

class Sender(object):
  def __init__(self,h_id,hh_id,\
    server_address,zmq_port):
    self.h_id=h_id
    self.hh_id=hh_id
    self.server_address=server_address
    self.zmq_port=zmq_port
  
  def serialize(self,msg):
    return str(self.h_id) + ' ' + json.dumps(msg) 

  def run(self):
    print('Thread:%s for sending data for hh_id:%d started\n'%\
      (threading.current_thread().name,self.hh_id))
    self.send()

  def send(self):
    context= zmq.Context()
    pub= context.socket(zmq.PUSH)
    pub.connect('tcp://%s:%d'%(self.server_address,self.zmq_port))

    curated_plugs= glob.glob('%s/%s/%s/curated_*.csv'% \
      (common.datadir,self.h_id,self.hh_id))
    plugs = { plug_file.partition('curated_')[-1].rpartition('.')[0] : open(plug_file,'r') \
      for plug_file in curated_plugs }
  
    for time_step in range(common.time_steps[0],\
      common.time_steps[1]+common.step_size,common.step_size):

      t_struct=time.gmtime(time_step) 
      index= (t_struct.tm_hour*60*60 + t_struct.tm_min*60 + t_struct.tm_sec)//common.step_size
      msg={'ts':time_step, 'weekday': t_struct.tm_wday,'index': index,\
        'h_id':self.h_id, 'hh_id':self.hh_id, 'plugs':{}}
      hh_load=0
      for plug_id,f in plugs.items():
        plug_load=float(f.readline().rstrip().split(',')[1])
        msg['plugs'][plug_id]=plug_load
        hh_load+=plug_load

      msg['hh_load']=hh_load

      pub.send_string(self.serialize(msg))
      time.sleep(.1)

    (plug.close() for plug in plugs.values())
