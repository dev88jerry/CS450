"""
CS450 Final
Katya Griffiths-Julien
Jerry Lau

Problem I: MapReduce implementation using Python and mrjob
"""

# Mean Income

from mrjob.job import MRJob
import sys

class meanIncomes(MRJob):
    def mapper(self, _, line):
        for item in line.split(','):
            yield None, float(item)

    def reducer(self, _, items):
        incomes = list(items)
        n = len(incomes)
        mean = sum(incomes) / n
        print(f"Mean of Incomes: {mean}")
        yield None, mean
       
        