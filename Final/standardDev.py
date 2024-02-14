"""
CS450 Final
Katya Griffiths-Julien
Jerry Lau

Problem I: MapReduce implementation using Python and mrjob
"""

# Standard Deviation

from mrjob.job import MRJob
import sys

class stdDevIncomes(MRJob):
    def mapper(self, _, line):
        for item in line.split(','):
            yield None, float(item)

    def reducer(self, _, items):
        incomes = list(items)
        n = len(incomes)
        mean = sum(incomes) / n
        variance = sum((x - mean)**2 for x in incomes) / n
        std_dev = variance**0.5
        print(f"Standard Deviation of Incomes: {std_dev}")
        yield None, std_dev
		
 