from mrjob.job import MRJob

class TempMinMax(MRJob):
  # Step 1
  def mapper(self, _, line):
    locale, _, valueType, value, _, _, _, _ = line.split(',')

    if valueType.lower() in ('tmin', 'tmax'):
      yield locale, float(value)

  def reducer(self, key, values):
    items = list(values)

    yield key, (min(items), max(items))

if __name__ == '__main__':
  TempMinMax.run()