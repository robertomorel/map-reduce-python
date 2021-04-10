from mrjob.job import MRJob
from mrjob.step import MRStep

class PopHero(MRJob):
  def steps(self):
    # Retorno de fluxo dos steps
    return [
      MRStep(mapper=self.mapper, reducer=self.reducer),
      MRStep(reducer=self.reducer2),
    ]

  # Step 1
  def mapper(self, _, line):
    tokens = line.split(' ')

    yield tokens[0], len(tokens) - 2
    
  def reducer(self, key, values):
    items = list(values)

    yield None, (sum(items), key) 

  ########################################################################
  # Step 2
  def reducer2(self, key, values):
    items = list(values)
    
    items.sort(reverse=True)

    for tupla in items:
      yield 'Hero: {0}'.format(tupla[1]), 'No of Friends: {0}'.format(tupla[0])  

if __name__ == '__main__':
  PopHero.run()