from mrjob.job import MRJob
from mrjob.step import MRStep
from recommendation_store import RecommendationStore

import json

class CWBTIAB(MRJob):
  # Para achar o IP do Redis: docker inspect b00ebb4162c1 
  #redis = RecommendationStore('localhost', '6379', 0) # 0 eh o banco default

  def steps(self):
    # Retorno de fluxo dos steps
    return [
      MRStep(mapper=self.mapper, reducer=self.reducer),
      MRStep(mapper=self.mapper_phase2, reducer=self.reducer_phase2),
    ]   

  def mapper(self, _, line):
    userID, item = line.split(',')
    yield userID, item

  def reducer(self, key, values):
    items = list(values)
    yield key, items

  # Montar os Stripes
  def mapper_phase2(self, key, value):
    items = value

    items.sort()

    for i in range (len(items)):
      item1 = items[i]

      m = {}
      for j in range(i + 1, len(items)):
        item2 = items[j]
        if item2 not in m:
          m[item2] = 0

        m[item2] = m[item2] + 1

      yield item1, m

  def reducer_phase2(self, key, values):
    stripes = list(values)
    
    final = {}
    for m in stripes:
      for k, v in m.items():
        if k not in final:
          final[k] = 0

        final[k] = final[k] + v

    #self.redis.save_recommendation(key, json.dumps(final))
    yield key, final        

if __name__ == "__main__":
  CWBTIAB.run()        