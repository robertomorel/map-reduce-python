from mrjob.job import MRJob

class FriendsByAge(MRJob):
  # Step 1
  def mapper(self, _, line):
    userId, name, age, number_of_friends = line.split(',')

    age = int(age)

    category = None
    if age in range(0, 25): category = '<25'
    elif age in range(25, 35): category = '26-35'
    elif age in range(35, 55): category = '36-55'
    elif age in range(55, 100): category = '>56'

    yield category, (float(number_of_friends), userId)

  def reducer(self, key, values):
    items = list(values)
    #avg = sum(items) / len(items)
    yield key, (min(items), max(items))

if __name__ == '__main__':
  FriendsByAge.run()