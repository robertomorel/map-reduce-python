from mrjob.job import MRJob

class MBA(MRJob):
  def get_combinations(self, items):
    result = []

    for i in range(len(items)):
      for j in range(i + 1, len(items)):
        a = items[i]
        b = items[j]

        result.append((a, b))

    return result    

  def mapper(self, _, line):
    items = line.split(',')
    # Ordena
    items.sort()

    # Gera combinacoes
    combinations = self.get_combinations(items)
    for comb in combinations:
      yield comb, 1

  def reducer(self, key, values):
    items = list(values)
    yield key, sum(items)

if __name__ == "__main__":
  MBA.run()        