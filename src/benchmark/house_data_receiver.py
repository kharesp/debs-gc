import zmq,json,pymongo,common,argparse
from pymongo import MongoClient

class Receiver(object):
  def __init__(self,h_id,zmq_port):
    self.h_id=h_id 
    self.client= MongoClient()
    self.db= self.client['load_%s'%(self.h_id)]
    self.hh_collections=set()
    self.zmq_port=zmq_port

  def deserialize(self,msg):
    delimiter= msg.find('{')
    topic= msg[0:delimiter].strip()
    msg= json.loads(msg[delimiter:])
    return topic, msg

  def check_collection_exists(self,collection_name):
    if collection_name in self.hh_collections:
      return
    else:
      if collection_name in self.db.collection_names():
        self.hh_collections.add(collection_name)
      else:
        self.db.create_collection(collection_name) 
        self.db[collection_name].create_index([('ts',pymongo.ASCENDING)])
        print('created collection %s in db\n'%collection_name)
        self.hh_collections.add(collection_name)
        
  def insert(self,msg):
    collection_name='%s_%s'%(msg['hh_id'],msg['h_id'])
    self.check_collection_exists(collection_name)
    expired_ts= msg['ts']-common.window_size
    res=self.db['0_0'].delete_many({'ts': {'$lt': expired_ts}})
    if (res.deleted_count>0):
      print('Deleting %d data samples\n'%res.deleted_count)
    self.db[collection_name].insert_one(msg)
 
  def print_collection_contents(self,collection_name):
    cursor=self.db[collection_name].find()
    for document in cursor:
      print(document)


  def receive(self):
    context= zmq.Context()
    sub= context.socket(zmq.PULL)
    sub.set_hwm(common.hwm)
    sub.bind('tcp://*:%d'%self.zmq_port)
    #sub.setsockopt_string(zmq.SUBSCRIBE, str(self.h_id))
   
    print('Receiver for house:%d started\n'%self.h_id)
    while True:
      topic,msg= self.deserialize(sub.recv_string())
      self.insert(msg)

if __name__ == "__main__":
  parser= argparse.ArgumentParser(description='House data receiver')
  parser.add_argument('h_id',type=int, help='h_id of house for which data will be received')
  parser.add_argument('zmq_port',type=int, help='zmq port on which to receive house data')
  args=parser.parse_args()

  Receiver(h_id=args.h_id,zmq_port=args.zmq_port).receive()
