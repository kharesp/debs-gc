from heap import Heap

class StreamingMedian(object):
  
  def __init__(self):
    self.left=Heap()
    self.right=Heap(max=False)
    self.median=0.0

  def get_median(self):
    return self.median

  def insert(self,value):
    #Both max and min heaps have the same number of elements
    if(self.left.count()==self.right.count()):
      if(value > self.median):
        self.right.insert(value)
        self.median=self.right.top()
      else:
        self.left.insert(value)
        self.median=self.left.top()
   
    #Left heap has more elements 
    elif(self.left.count() > self.right.count()):
      if (value > self.median):
        self.right.insert(value)
      else:
        self.right.insert(self.left.deleteTop())
        self.left.insert(value)

      self.median= (self.right.top()+self.left.top())/2
  
    #Right heap has more elements
    else:
      if (value > self.median):
        self.left.insert(self.right.deleteTop())
        self.right.insert(value)
      else:
        self.left.insert(value)

      self.median= (self.right.top()+self.left.top())/2

if __name__ == "__main__":
  container=StreamingMedian()
  values=[5,15,1,3,2,8,7,9,10,6,11,4]
  for v in values:
    container.insert(v)
    m=container.get_median()
    print(m)
