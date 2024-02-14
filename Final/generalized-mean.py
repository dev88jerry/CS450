"""
CS450 Final
Katya Griffiths-Julien
Jerry Lau

Problem I: MapReduce implementation using Python and mrjob
"""

# Generalized Mean

from mrjob.job import MRJob
import sys

class generalizedMeanIncomes(MRJob):
    def configure_args(self):
        super(generalizedMeanIncomes, self).configure_args()
        self.add_passthru_arg('--order', type=float, help='Order of the generalized mean')

    def mapper(self, _, line):
        for item in line.split(','):
            yield None, float(item)

    def reducer(self, _, items):
        incomes = list(items)
        n = len(incomes)
        p = self.options.order
        generalized_mean = (sum(x**p for x in incomes) / n)**(1/p)
        print(f"Generalized Mean (p={p}): {generalized_mean}")
        yield None, generalized_mean
       
        