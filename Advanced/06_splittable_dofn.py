"""
Advanced 06 — Splittable DoFns (SDF)

Source: https://beam.apache.org/documentation/programming-guide/#splittable-dofns

A Splittable DoFn (SDF) is the modern, recommended way to build
custom I/O connectors in Beam. Unlike a regular DoFn (one call per element),
an SDF can:
  - Process one logical "work item" in pieces (e.g., a 10 GB file in 64 MB chunks)
  - Be dynamically split mid-execution by the runner for better parallelism
  - Report progress so the runner knows how much work remains
  - Work correctly with both batch and streaming runners

Core concepts:
  @beam.DoFn.RestrictionProvider  → defines how to split work
  OffsetRange / OffsetRangeTracker → built-in restriction for byte/record offsets
  @process                         → takes element + restriction_tracker
  restriction_tracker.try_claim()  → claim the next unit of work

Real-world SDFs in the Beam SDK: ReadFromBigQuery, ReadFromPubSub,
ReadFromKafka, ReadFromParquet — they are all SDFs under the hood.

Run:
  python Advanced/06_splittable_dofn.py
"""

import apache_beam as beam
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker
from apache_beam.transforms.core import RestrictionProvider


# ── SDF Example: Read a list in configurable chunks ───────────────────────
# This mirrors what a real file-reading SDF does: split a byte range
# and process one chunk at a time.

class ReadInChunksDoFn(beam.DoFn, RestrictionProvider):
    """
    SDF that processes a Python list in chunks.
    Each 'element' is a list; the restriction is an OffsetRange over its indices.

    This is a simplified teaching example — a real SDF would read
    byte ranges from a file or rows from a database table.
    """

    def __init__(self, chunk_size: int = 3):
        self.chunk_size = chunk_size

    # ── RestrictionProvider methods ───────────────────────────────────────

    def initial_restriction(self, element):
        """The full range of work for this element."""
        return OffsetRange(0, len(element))

    def create_tracker(self, restriction):
        """Returns the tracker object that guards claim() calls."""
        return OffsetRestrictionTracker(restriction)

    def restriction_size(self, element, restriction):
        """Tells the runner how much work is in this restriction (for progress)."""
        return restriction.stop - restriction.start

    def split(self, element, restriction):
        """
        Called by the runner to split work into smaller pieces.
        Yield OffsetRange objects. The runner calls this on the initial
        restriction before (and sometimes during) execution.
        """
        start = restriction.start
        stop  = restriction.stop
        while start < stop:
            yield OffsetRange(start, min(start + self.chunk_size, stop))
            start += self.chunk_size

    # ── DoFn.process ─────────────────────────────────────────────────────

    def process(
        self,
        element,
        restriction_tracker=beam.DoFn.RestrictionParam(None),
    ):
        """
        Process elements in the claimed range.
        try_claim(pos) returns True as long as this worker owns the position.
        Once another worker steals the restriction, try_claim returns False.
        """
        restriction = restriction_tracker.current_restriction()
        pos = restriction.start

        while restriction_tracker.try_claim(pos):
            yield element[pos]
            pos += 1


def run():
    # Each element is a list; the SDF will process them in chunks.
    datasets = [
        list(range(10)),           # 0–9
        ["a", "b", "c", "d", "e"],
    ]

    with beam.Pipeline() as p:
        result = (
            p
            | "CreateDatasets" >> beam.Create(datasets)
            | "ReadInChunks"   >> beam.ParDo(ReadInChunksDoFn(chunk_size=3))
        )
        result | "Print" >> beam.Map(lambda x: print(f"  element: {x}"))


# ── SDF skeleton for a real file reader (reference template) ─────────────

class FileReaderSDF(beam.DoFn, RestrictionProvider):
    """
    Template for building a production file-reading SDF.
    Replace the placeholder sections with real I/O logic.
    """

    CHUNK_BYTES = 64 * 1024 * 1024  # 64 MB

    def initial_restriction(self, file_path: str):
        import os
        file_size = os.path.getsize(file_path)  # or use GCS metadata
        return OffsetRange(0, file_size)

    def create_tracker(self, restriction):
        return OffsetRestrictionTracker(restriction)

    def restriction_size(self, element, restriction):
        return restriction.stop - restriction.start

    def split(self, element, restriction):
        start = restriction.start
        while start < restriction.stop:
            yield OffsetRange(start, min(start + self.CHUNK_BYTES, restriction.stop))
            start += self.CHUNK_BYTES

    def process(self, file_path: str, restriction_tracker=beam.DoFn.RestrictionParam(None)):
        restriction = restriction_tracker.current_restriction()
        byte_offset = restriction.start

        # Open file, seek to byte_offset, read until try_claim fails
        with open(file_path, "rb") as f:
            f.seek(byte_offset)
            while restriction_tracker.try_claim(byte_offset):
                line = f.readline()
                if not line:
                    break
                yield line.decode("utf-8").strip()
                byte_offset = f.tell()


if __name__ == "__main__":
    print("=== Splittable DoFn: Read list in chunks ===")
    run()
