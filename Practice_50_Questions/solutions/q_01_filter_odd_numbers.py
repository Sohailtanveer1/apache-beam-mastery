"""
Solution — Q1: Filter odd numbers

Write a pipeline that reads a list of integers from beam.Create,
filters out all even numbers, and prints the remaining odd numbers.
"""

import apache_beam as beam


def run():
    with beam.Pipeline() as p:
        (
            p
            | "CreateIntegers" >> beam.Create([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
            | "FilterOdds"     >> beam.Filter(lambda n: n % 2 != 0)
            | "Print"          >> beam.Map(print)
        )


if __name__ == "__main__":
    run()
