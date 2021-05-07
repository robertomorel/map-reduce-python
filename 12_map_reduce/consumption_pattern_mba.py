from mrjob.job import MRJob
from itertools import combinations 
from mrjob.step import MRStep
from consumption_store import ConsumptionStore

import json

class ConsumptionPattern(MRJob):

  SORT_VALUES = True
  redis = RecommendationStore('localhost', '6379', 0) # 0 eh o banco default

  def steps(self):
    return [
      MRStep(mapper=self.mapper1,reducer=self.reducer1),
      MRStep(mapper=self.mapper2, combiner=self.combiner, reducer=self.reducer2),
      MRStep(reducer=self.reducer3),
    ]

  def mapper1(self,_ ,line):
    tokens = line.split(';')
    yield tokens[0], tokens[1]
    
  def reducer1(self, key, values):
    valor = list(values)
    yield key, valor    

  def mapper2(self, key, values):
    items = values
    items.sort()

    combs = combinations(items, 2)
    for comb in combs:
      yield comb, 1

  def combiner(self, key, values):
    yield key, sum(values)

  def reducer2(self, key, values):
    soma = sum(values)
    yield key[0], (key[1], soma)

  def reducer3(self, key, values):
    items = list(values)

    self.redis.save_consumption(key, json.dumps(items))
    yield key,items

if __name__ == '__main__':
  ConsumptionPattern.run()