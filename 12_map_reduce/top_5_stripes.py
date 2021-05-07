from mrjob.job import MRJob
from mrjob.step import MRStep
from consumption_store import ConsumptionStore

import json

class Top5WithStripes(MRJob):

  #redis = RecommendationStore('localhost', '6379', 0) # 0 eh o banco default

  def steps(self):
    return[
      MRStep(mapper=self.mapper1,reducer=self.reducer1),
      MRStep(mapper=self.mapper2,reducer=self.reducer2),
      MRStep(mapper=self.mapper3),
    ]

  def mapper1(self,_ , line):
    token = line.split(';')
    yield token[3], token[1]
    
  def reducer1(self, key, values):
    valor = list(values)
    yield key, valor

  def mapper2(self, key, value):
    items = value
  
    items.sort()
  
    for i in range(len(items)):
      item1 = items[i]

      m = {}
      for j in range (i + 1, len(items)):
        item2 = items[j]
        if item2 not in m:
          m[item2] = 0
          m[item2] = m[item2] + 1
      yield item1,m
        
  def reducer2(self, key, values):
    stripes = list(values)

    final = {}
    for a in stripes:
      for x,z in a.items():
        if x not in final:
          final[x] = 0
          
        final[x] = final[x] + z

    #self.redis.save_consumption(key, json.dumps(items))
    yield key, final

  def mapper3(self, key, value):
    list_result = {k:v for k,v in sorted(value.items(),key=lambda item: item[1])}
    values = list(list_result.keys())
    interator = len(list_result)
    for value in values:
      if interator > 5:
        list_result.pop(value)
        interator = interator -1

    list_result = {k:v for k,v in sorted(list_result.items(),key=lambda item: item[1],reverse=True)}
    yield key, list_result  

if __name__ == '__main__':
    Top5WithStripes.run()