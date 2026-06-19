"""
Solution — Q2: ParDo filter words longer than 3 characters

Write a DoFn that takes a list of words and emits only words
strictly longer than 3 characters.
"""

import apache_beam as beam


class FilterLongWordsDoFn(beam.DoFn):
    def __init__(self, min_length: int = 3):
        self.min_length = min_length

    def process(self, word):
        if len(word) > self.min_length:
            yield word


def run():
    words = ["go", "beam", "is", "great", "at", "scale"]

    with beam.Pipeline() as p:
        (
            p
            | "CreateWords"      >> beam.Create(words)
            | "FilterLongWords"  >> beam.ParDo(FilterLongWordsDoFn(min_length=3))
            | "Print"            >> beam.Map(print)
        )


if __name__ == "__main__":
    run()
