from mrjob.job import MRJob

class AmountsByCustomer(MRJob):
  def mapper(self, _, line):
    userId, _, amount = line.split(',')
    yield userId, float(amount)

  def reducer(self, key, values):
    items = list(values)
    
    minVar = min(items)
    maxVar = max(items)
    avg = sum(items) / len(items)

    # Somatorio por userId
    #yield key, sum(items)
    # Valor maximo por userId
    #yield key, maxVar
    # Valor minimo por userId
    #yield key, minVar
    # Retornando como valor uma tupla max e min por userId
    #yield key, (minVar, maxVar)
    # Retornando como valor uma tupla max, min e media por userId
    yield key, (minVar, maxVar, avg)

if __name__ == '__main__':
  AmountsByCustomer.run()        