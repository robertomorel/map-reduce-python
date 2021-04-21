from mrjob.job import MRJob
from mrjob.step import MRStep

class SumOfSalesByCountry(MRJob):
  def steps(self):
    # Retorno de fluxo dos steps
    return [
      MRStep(mapper=self.mapper, reducer=self.reducer),
      MRStep(reducer=self.reducer2),
      MRStep(reducer=self.reducer3),
    ]

  def mapper(self, _, line):
    tokens = line.split(',')

    country = tokens[7]
    product = tokens[1]

    yield (country, product), 1

  # Sumariza qnde de produtor indexado por COUNTRY;PRODUCT
  def reducer(self, key, values):
    items = list(values)
    yield key, sum(items)

  # Cria lista sumarizada por COUNTRY contendo PRODUCT e valor sumarizado
  def reducer2(self, key, values):
    items = list(values)
    yield key[0], list([key[1], items])

  # Unifica as listas por COUNTRY
  def reducer3(self, key, values):
    items = list(values)
    yield key, items

if __name__ == "__main__":
  SumOfSalesByCountry.run()    