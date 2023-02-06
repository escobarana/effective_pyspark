from pyspark.sql import SparkSession

from exercises.a_spark_demo.f_common_business_logic_methods import (
    ranges_overlap,
)

spark = SparkSession.builder.getOrCreate()


def test_overlapping_ranges():
    # Note the symmetry too: this DataFrame could've been reduced to half the size,
    # though that's more of a "hey look how beautiful mathematics is".
    df = spark.createDataFrame(
        [
            (0, 5, -5, -1),
            (0, 5, -5, 2),
            (0, 5, -5, 7),
            (0, 5, 1, 7),
            (0, 5, 6, 7),
            (0, 5, 2, 3),
            (0, 5, -5, None),
            (0, 5, 1, None),
            (0, 5, 6, None),
            (0, None, -5, -1),
            (0, None, -5, 1),
            (0, None, 5, 10),
        ],
        schema=list("abcd"),
    )

    result = df.withColumn(
        "are overlapping ranges",
        ranges_overlap(*[df[x] for x in list("abcd")]),
    )

    # Run 'pytest -s this_file' to see the output when no errors are thrown in a test.
    result.show()
