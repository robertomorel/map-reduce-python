from mrjob.job import MRJob
from mrjob.step import MRStep

class FriendsByAge(MRJob):
  def steps(self):
    # Retorno de fluxo dos steps
    return [
      MRStep(mapper=self.mapper, reducer=self.reducer),
      MRStep(reducer=self.reducer2),
    ]

  # Step 1
  def mapper(self, _, line):
    userId, name, age, number_of_friends = line.split(',')
    yield age, float(number_of_friends)

  def reducer(self, key, values):
    items = list(values)
    #yield key, (sum(items) / len(items))
    age = key
    avg = sum(items) / len(items)

    #Gerando as tuplas para que o criterio de ordanacao fique primeiro
    yield None, (avg, age)

  # Step 2
  def reducer2(self, key, values):
    items = list(values)
    #Ordena pelo primeiro componente. Caso de empate, ordena pelo segundo.
    items.sort()

    first = items[0]
    #yield key, len(items)
    #yield key, first
    yield first[1], first[0]

if __name__ == '__main__':
  FriendsByAge.run()