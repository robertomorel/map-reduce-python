from mrjob.job import MRJob
from mrjob.step import MRStep

class TopN(MRJob):
  top = []
  N = 5

  def steps(self):
    return [
      MRStep(mapper=self.mapper, reducer_init=self.reducer_init),
      MRStep(reducer=self.reducer),
    ]

  def mapper(self, _, line):
    weight, catId, name = line.split(",")

    weight = float(weight)
    self.top.append((weight, name))

    if len(self.top) > self.N:
      self.top.sort() # Ordena a lista
      self.top.pop(0) # Apaga primeiro elemento do array

  # Metodo do MRJob que executa antes do reducer
  # Obriga o uso de steps para explicitar o mrjob.reducer.init()
  def reducer_init(self):
    for item in self.top:
      yield None, item

  def reducer(self, key, values):
    items = list(values)
    items.sort(reverse=True)

    for i in range(self.N):
      item = items[i]
      weight = item[0]
      name = item[1]

      yield name, weight


if __name__ == "__main__":
  TopN.run()    