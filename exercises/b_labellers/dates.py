import datetime
import pandas as pd

from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import date_format, dayofweek, udf

import holidays


def is_belgian_holiday(date: datetime.date) -> bool:
    holiday_list = []
    res = False
    for holiday in holidays.Belgian(years=[2019, 2020, 2021, 2022]).items():
        holiday_list.append(holiday)

    if date in holiday_list:
        res = True
    
    return True


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    """Adds a column indicating whether or not the attribute `colname`
    in the corresponding row is a weekend day."""
    # return frame.withColumn(new_colname, date_format(frame[colname], 'EEE').isin(["Sat", "Sun"]).cast("boolean"))
    return frame.withColumn(new_colname, dayofweek(frame[colname]).isin(1, 7))
    
holiday_udf = udf(is_belgian_holiday, BooleanType())

def label_holidays(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether or not the column `colname`
    is a holiday."""
    return frame.withColumn(new_colname, holiday_udf(frame[colname]))


def label_holidays2(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether or not the column `colname`
    is a holiday. An alternative implementation."""
    
    pass


def label_holidays3(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether or not the column `colname`
    is a holiday. An alternative implementation."""
    pass
