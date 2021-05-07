from mrjob.job import MRJob
#from itertools import combinations 
from mrjob.step import MRStep

class MBA(MRJob):

  # Ordena o value da ultima saida
  SORT_VALUES = True

  def steps(self):
    # Retorno de fluxo dos steps
    return [
      MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer),
      MRStep(reducer=self.reducer2),
    ]

  def get_combinations(self, items):
    result = []

    # Ordena
    items.sort()

    # Gera combinacoes
    for i in range(len(items)):
      for j in range(i + 1, len(items)):
        for k in range(j + 1, len(items)):
          a = items[i]
          b = items[j]
          c = items[k]

          result.append((a, b, c))
          #result.append((a, b))

    return result    

  def mapper(self, _, line):
    items = line.split(',')
    
    #combs = combinations(items, 3)
    combinations = self.get_combinations(items)
    for comb in combinations:
      yield comb, 1

  def combiner(self, key, values):
    yield key, sum(values)
        
  def reducer(self, key, values):
     yield None, (key, sum(values))

  def reducer2(self, key, values):
    items = list(values)
    #items.sort()

    for item in items:
      yield item[0], item[1]  

if __name__ == "__main__":
  MBA.run()