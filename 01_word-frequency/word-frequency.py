from mrjob.job import MRJob

class WordFrequency(MRJob):
  def mapper(self, _, line):
    words = line.split()
    for word in words:
      yield word, 1

  def reducer(self, key, values):
    # Soma valor das tuplas com mesma chave
    yield key, sum(values)

if __name__ == '__main__':
  WordFrequency.run()        