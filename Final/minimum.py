"""
CS450 Final
Katya Griffiths-Julien
Jerry Lau

Problem I: MapReduce implementation using Python and mrjob
"""

# Min Income

from mrjob.job import MRJob
import sys

class minIncome(MRJob):
    def mapper(self, _, line):
        for item in line.split(','):
            yield None, float(item)

    def reducer(self, _, items):
        min_income = min(items)
        print(f"Minimum Income: {min_income}")
        yield None, min_income
 
 
 