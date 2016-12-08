import common
from pymongo import MongoClient
from functools import reduce
from LoadModel_V1 import LoadModel

class Processor(object):
  def __init__(self):
    self.client=MongoClient()

  def process(self,query):
    if (query['query_type']== common.QueryType.load.name):
      self.load(query)
    elif (query['query_type']== common.QueryType.outlier.name):
      self.outlier(query)
    else:
      print('query_type:%s not recognized\n'%query['query_type'])

  def load(self,query):
    print('Received load query for level:%s, granularity:%s for traget:%s\n'%\
      (query['query_level'],query['granularity'],query['query_target']))

    if(query['query_level']==common.QueryLevel.plug.name):
      self.plug_level_load_prediction(query)
    elif (query['query_level']==common.QueryLevel.household.name):
      self.household_level_load_prediction(query)
    elif (query['query_level']==common.QueryLevel.house.name):
      self.house_level_load_prediction(query)
    else:
      print('invalid query level')

  def plug_level_load_prediction(self,query):
    target_plug=query['query_target']
    plug_id,hh_id,h_id=target_plug.split('_')
    cursor=self.client['load_%s'%h_id]['%s_%s'%(hh_id,h_id)].find()
    DT=[]
    V=[]
    X=[]
    for reading in cursor:
      DT.append([reading['weekday'],reading['index']])
      V.append(reading['plugs'][target_plug])

    modified_DT,modified_V= self.\
      curate_for_granularity(query['granularity'],DT,V) 
    print(modified_DT)
    print(modified_V)

    [X.append([modified_V[i-1]] + modified_DT[i]) \
      for i in range(1,len(modified_DT))]
    Y=modified_V[1:]
    self.train(X,Y)

  def household_level_load_prediction(self,query):
    target_hh=query['query_target']
    hh_id,h_id=target_hh.split('_')
    cursor=self.client['load_%s'%h_id][target_hh].find()
    DT=[]
    V=[]
    X=[]
    for reading in cursor:
      DT.append([reading['weekday'],reading['index']])
      V.append(reading['hh_load'])

    modified_DT,modified_V= self.\
      curate_for_granularity(query['granularity'],DT,V) 

    [X.append([modified_V[i-1]] + modified_DT[i]) \
      for i in range(1,len(modified_DT))]
    Y=modified_V[1:]
    self.train(X,Y)

  def house_level_load_prediction(self,query):
    target_house=query['query_target']
    hh_collections=self.client['load_%s'%target_house].collection_names()
    cursors=[self.client['load_%s'%target_house][col].find()\
      for col in hh_collections]
    cursor_0=cursors[0]
    cursors_rem=cursors[1:]
    DT=[] 
    V=[]
    X=[]
    for i in range(0,cursor_0.count()):
      curr_elem=cursor_0.next() 
      DT.append([curr_elem['weekday'],curr_elem['index']])
      V.append(curr_elem['hh_load'] + reduce(lambda x,y: x+y, \
        [c.next()['hh_load'] for c in cursors_rem]))

    modified_DT,modified_V= self.\
      curate_for_granularity(query['granularity'],DT,V) 

    [X.append([modified_V[i-1]] + modified_DT[i]) \
      for i in range(1,len(modified_DT))]
    Y=modified_V[1:]
    self.train(X,Y)

  def curate_for_granularity(self,granularity,DT,V):
    if (granularity==common.Granularity.win_5mins.name):
      return DT,V
    else:
      modified_DT=[]
      modified_V=[]
      granularity_value=common.Granularity[granularity].value
      num_elems=len(DT)//granularity_value
      for i in range(0,num_elems*granularity_value,granularity_value):
        modified_DT.append(DT[i])
        modified_V.append(reduce(lambda x,y:x+y,\
          [V[idx] for idx in range(i,i+granularity_value)]))
      return modified_DT,modified_V

  def train(self,X,Y):
    gp_model=LoadModel(X,Y)
    hyperparams=gp_model.train_model()

  def outlier(self,query):
    print('Received outlier query')
