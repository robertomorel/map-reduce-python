from mrjob.job import MRJob

##########################################################################
# Quero saber a minima temperatura maxima e maxima temperatura minima ####
##########################################################################

class TempMinMax(MRJob):
  # Step 1
  def mapper(self, _, line):
    tokens = line.split(',')

    location = tokens[0]
    metric = tokens[2]
    value = tokens[3]

    if metric == 'TMIN' or metric == 'TMAX':
      # Agrupar por localizaca e metrica
      yield (location, metric), float(value)

  def reducer(self, key, values):
    items = list(values)
    location = key[0]
    metric = key[1]

    # Menor temp. observada
    if metric == 'TMAX':
      yield location, min(items)
    else: 
      yield location, max(items)  

if __name__ == '__main__':
  TempMinMax.run()