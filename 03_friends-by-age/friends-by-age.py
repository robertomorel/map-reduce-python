from mrjob.job import MRJob

class FriendsByAge(MRJob):

  def mapper(self, _, line):
    userId, name, age, number_of_friends = line.split(',')
    yield age, float(number_of_friends)

  def reducer(self, key, values):
    items = list(values)
    avg = sum(items) / len(items)
    # Média de friends por age
    yield key, avg


if __name__ == '__main__':
  FriendsByAge.run()