"""
CS450 Final
Katya Griffiths-Julien
Jerry Lau

Problem I: MapReduce implementation using Python and mrjob
"""

# Total Incomes

from mrjob.job import MRJob
import sys

class totalIncomes(MRJob):
    def mapper(self, _, line):
        for item in line.split(','):
            yield None, float(item)

    def reducer(self, _, items):
        total_sum = sum(items)
        yield None, total_sum
        print(f"Sum of Incomes: {total_sum}")
        
        