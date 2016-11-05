from array import array

class Heap(object):
  def __init__(self,max=True):
    self.maxHeap=max
    self.values=array('d')

  def left(self,i):
    return 2*i +1
  
  def right(self,i):
    return 2*(i+1)

  def parent(self,i):
    if (i <= 0):
      return -1
    else:
      return (int)(i-1)//2

  def compare(self,lhs,rhs):
    if (self.maxHeap):
      return lhs>rhs;
    else:
      return lhs<rhs;

  def exchange(self,left_idx,right_idx):
    temp=self.values[left_idx];
    self.values[left_idx]=self.values[right_idx]
    self.values[right_idx]=temp
    
  def heapify(self,i):
    parent_index= self.parent(i)
    if( parent_index>=0 and self.compare(self.values[i],self.values[parent_index])):
      self.exchange(i,parent_index)
      self.heapify(parent_index)
 
  def count(self):
    return len(self.values) 


  def top(self):
    if (self.count()>0):
      return self.values[0]
    else:
      return None 

  def deleteTop(self):
    topElem=None

    if (self.count()>0):
      topElem=self.values[0]
      self.exchange(0,self.count()-1)
      self.values.pop()
      self.heapify(self.parent(self.count()))

    return topElem

  def insert(self,value):
    self.values.append(value)
    self.heapify(self.count()-1)

  def print(self):
    print(self.values)
