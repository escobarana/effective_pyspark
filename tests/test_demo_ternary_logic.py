import pyspark.sql.functions as sf
from pyspark.sql import SparkSession, DataFrame


spark = SparkSession.builder.getOrCreate()


def apply_boolean_operators(frame: DataFrame) -> DataFrame:
    return (
        frame.withColumn("a or b", sf.col("a") | sf.col("b"))
        .withColumn("a and b", sf.col("a") & sf.col("b"))
        .withColumn("not a", ~sf.col("a"))
    )


def test_ternary_logic():
    """For those wanting to understand how Spark (and many other tools, and by
    extension, programming languages) work in the face of ternary (True, False,
    null) logic, run this test with pytest, passing the -s option to prevent
    pytest from "swallowing" the output printed to stdout."""
    df = spark.createDataFrame(
        [
            (None, True),
            (None, False),
            (None, None),
            (True, True),
            (True, False),
            (True, None),
            (False, True),
            (False, False),
            (False, None),
        ],
        schema=list("ab"),
    )

    result = apply_boolean_operators(df)

    result.show()
    # This isn't an actual test: it's missing an assertion.  The purpose of
    # this piece of code is to make you think before looking at the expected
    # value: what does 'None and True' evaluate to, do you think?
