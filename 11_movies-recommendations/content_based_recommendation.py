from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations 

import sys
from math import*

import statistics

class ContentBasedRecommendation(MRJob):
    SORT_VALUES = True

    def steps(self):
        return [
            MRStep(mapper=self.mapper_phase1, reducer=self.reducer_phase1),
            MRStep(mapper=self.mapper_phase2, reducer=self.reducer_phase2),
            MRStep(mapper=self.mapper_phase3, reducer=self.reducer_phase3),
            MRStep(mapper=self.mapper_phase4, reducer=self.reducer_phase4)
        ]    

    def mapper_phase1(self, _, line):
        userId, movie, rating, _ = line.split(',')

        yield float(movie), (userId, float(rating))

    def reducer_phase1(self, key, values):
        movie = key

        items = list(values)
        numberOfRaters = len(items)

        for item in items:
            userId = item[0]
            rating = item[1]

            yield userId, (movie, rating, numberOfRaters)

    def mapper_phase2(self, key, value):
        (movie, rating, numberOfRaters) = value
        userId = key

        yield userId, (movie, rating, numberOfRaters)

    def reducer_phase2(self, key, values):
        userId = key
        items = list(values)

        items.sort()

        combs = combinations(items, 2)
        for comb in combs:
            # comb => ((movie1, rating1, number_of_raters1), (movie2, rating2, number_of_raters2))
            reducerKey = comb[0][0], comb[1][0]

            rating1 = comb[0][1]
            rating2 = comb[1][1]

            numberOfRaters1 = comb[0][2]
            numberOfRaters2 = comb[1][2]

            reducerValue = (rating1, numberOfRaters1, rating2, 
                numberOfRaters2)

            yield reducerKey, reducerValue

    def mapper_phase3(self, key, value):
        movie1, movie2 = key
        rating1, numberOfRaters1, rating2, numberOfRaters2 = value

        yield (movie1, movie2), (rating1, numberOfRaters1, rating2, numberOfRaters2)

    def reducer_phase3(self, key, values):
        items = list(values)

        movie1, movie2 = key

        values1 = []
        values2 = []

        for item in items:
            (rating1, numberOfRaters1, rating2, numberOfRaters2) = item

            values1.append(rating1)
            values2.append(rating2)

        min1 = min(values1)
        min2 = min(values2)
        max1 = max(values1)
        max2 = max(values2)

        avg1 = statistics.mean(values1)
        avg2 = statistics.mean(values2)
        g1 = statistics.geometric_mean(values1)
        g2 = statistics.geometric_mean(values2)
        h1 = statistics.harmonic_mean(values1)
        h2 = statistics.harmonic_mean(values2)

        features1 = [min1, max1, avg1, g1, h1]
        features2 = [min2, max2, avg2, g2, h2]

        euclidian = self.calculate_euclidian(features1, features2)
        cosine = self.cosine_similarity(features1, features2)

        yield (movie1, movie2), (euclidian , cosine)

    def mapper_phase4(self, key, value):
        movie1, movie2 = key

        yield movie1, (movie2, value)

    def reducer_phase4(self, key, values):
        items = list(values)

        yield key, items

    def calculate_euclidian(self, x, y):
        result = sqrt(sum(pow(a-b,2) for a, b in zip(x, y)))

        return 1 / (1 + result)

    def square_rooted(self, x):
        return round(sqrt(sum([a*a for a in x])),3)
 
    def cosine_similarity(self, x,y):
        numerator = sum(a*b for a,b in zip(x,y))
        denominator = self.square_rooted(x)*self.square_rooted(y)
        return round(numerator/float(denominator),3)

if __name__ == '__main__':
    ContentBasedRecommendation.run()