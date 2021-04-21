from mrjob.job import MRJob
from mrjob.step import MRStep

class MovingAverage(MRJob):
  window = 3

  def steps(self):
    return [
      MRStep(mapper=self.mapper, reducer_init=self.reducer_init),
      MRStep(reducer=self.reducer),
    ]

  def mapper(self, _, line):
    company, timestamp, value = line.split(',')
    yield company, (timestamp, float(value))

  # Metodo do MRJob que executa antes do reducer
  # Obriga o uso de steps para explicitar o mrjob.reducer.init()
  def reducer_init(self):
    pass

  def reducer(self, key, values):
    items = list(values)
    # Ordena pelo tempo
    items.sort()

    #for item in items:
    #  yield key, item

    #for i in range(len(items)):
    #  item = items[i]
    #  yield key, item

    # Calculando a media movel
    sum = 0.0

    for i in range(len(items)):
      item = items[i]
      timestamp = item[0]
      value = item[1]

      sum = sum + value

      if (i + 1) > self.window:
        sum = sum - items[i - self.window][1]    

      q = min(i + 1, self.window)  

      moving = sum / q

      yield key, (item, moving)
    


if __name__ == "__main__":
  MovingAverage.run()    