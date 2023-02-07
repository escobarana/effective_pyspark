import datetime

import holidays
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, dayofweek, udf
from pyspark.sql.types import BooleanType, DateType, StructField, StructType

MIN_YEAR_FOR_HOLIDAYS = 2000
MAX_YEAR_FOR_HOLIDAYS = 2020


def is_belgian_holiday(date: datetime.date) -> bool:
    belgian_holidays = holidays.BE()
    return date in belgian_holidays


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    # The line below is one solution. There is a more performant version of it
    # too, involving the modulo operator, but it is more complex. The
    # performance gain might not outweigh the cost of a programmer trying to
    # understand that arithmetic.
    # "SATURDAY = 7" and using those identifiers in the call to the `isin`
    # method.

    # Finally, programming languages and even frameworks within the same
    # programming language tend to differ in the convention whether Monday is
    # day 1 or not. You should always check the documentation corresponding to
    # your library.
    frame.withColumn(new_colname, dayofweek(colname).isin(1, 7))
    return frame


def label_holidays(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Add a column indicating whether or not the column `colname`
    is a holiday."""

    # holiday_udf = udf(lambda z: is_belgian_holiday(z), BooleanType())

    # If you were to see something like the line above in serious code, the
    # author of that line might not have understood the concepts of lambda
    # functions (nameless functions) and function references well. The
    # assignment above is more efficiently written as:
    holiday_udf = udf(is_belgian_holiday, BooleanType())

    return frame.withColumn(new_colname, holiday_udf(col(colname)))


def label_holidays2(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Add a column indicating whether or not the column `colname`
    is a holiday."""

    # A more efficient implementation of `label_holidays` than the udf-variant.
    # Major downside is that the range of years needs to be known a priori. Put
    # them in a config file or extract the range from the data beforehand.
    holidays_be = holidays.BE(
        years=list(range(MIN_YEAR_FOR_HOLIDAYS, MAX_YEAR_FOR_HOLIDAYS))
    )
    return frame.withColumn(
        new_colname, col(colname).isin(list(holidays_be.keys()))
    )


def label_holidays3(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Add a column indicating whether or not the column `colname`
    is a holiday."""

    # Another more efficient implementation of `label_holidays`. Same downsides
    # as label_holidays2, but scales better.
    holidays_be = holidays.BE(
        years=list(range(MIN_YEAR_FOR_HOLIDAYS, MAX_YEAR_FOR_HOLIDAYS))
    )

    # Since you're expecting a pyspark.sql.DataFrame in this function, you
    # *know* there's an existing SparkSession. You can get it like this:
    spark = SparkSession.getActiveSession()
    # alternatively, like this:
    # spark = frame.sql_ctx.sparkSession
    # Both return the same session. The latter is guaranteed to return a
    # SparkSession, the former only does it best effort and won't complain if
    # there's no active SparkSession. Static type checking tools, like mypy,
    # will raise warnings on that.

    holidays_frame = spark.createDataFrame(
        data=[(day, True) for day in holidays_be.keys()],
        schema=StructType(
            [
                StructField(colname, DateType(), False),
                StructField(new_colname, BooleanType(), False),
            ]
        ),
    )
    holidays_frame.show(51)
    holidays_frame.printSchema()
    part1 = frame.join(holidays_frame, on=colname, how="left")

    part2 = part1.na.fill(False, subset=[new_colname])
    part1.show()
    part2.show()
    part2.printSchema()
    return part2