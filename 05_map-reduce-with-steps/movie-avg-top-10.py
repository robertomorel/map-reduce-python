from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieReviewTop10(MRJob):
  def steps(self):
    # Retorno de fluxo dos steps
    return [
      MRStep(mapper=self.mapper, reducer=self.reducer),
      MRStep(reducer=self.reducer2),
    ]

  # Step 1
  def mapper(self, _, line):
    user_id, movie_id, rating, _ = line.split(',')

    yield movie_id, float(rating)
    
  def reducer(self, key, values):
    items = list(values)

    avg = avg = sum(items) / len(items)
    
    yield None, (avg, key) 

  ########################################################################
  # Step 2
  def reducer2(self, key, values):
    items = list(values)
    
    items.sort(reverse=True)

    for i in range(10):
      yield 'Average: {0}'.format(items[i][0]), 'Movie ID: {0}'.format(items[i][1])

if __name__ == '__main__':
  MovieReviewTop10.run()