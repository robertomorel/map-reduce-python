from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class MovingAverage(MRJob):
  window = 3

  def steps(self):
    return [
      MRStep(mapper=self.mapper, reducer=self.reducer),
      MRStep(reducer=self.reducer2),
    ]

  def mapper(self, _, line):
    tokens = line.split(',')

    country = tokens[7]
    product = tokens[1]
    date = tokens[0]
    price = tokens[2]
    payment_type = tokens[3]

    timestamp = datetime.strptime(date, '%m/%d/%y %H:%M').strftime("%m/%d/%Y")

    #if payment_type.lower() in (''):
    yield (country, product, timestamp), (float(price))

  def reducer(self, key, values):
    items = list(values)
    yield (key[0], key[1]), (key[2], sum(items))

  def reducer2(self, key, values):
    items = list(values)
    # Ordena pela data
    items.sort()

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

      yield key, (item[0], item[1], moving)
    
if __name__ == "__main__":
  MovingAverage.run()    