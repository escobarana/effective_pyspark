import datetime

from pyspark.sql import DataFrame
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import date_format, dayofweek, udf, create_map, lit, col

import holidays

def get_belgian_holidays():
    holiday_list = []
    for holiday in holidays.Belgium(years=[2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2018]).keys():
        holiday_list.append(holiday)
    
    return holiday_list

def is_belgian_holiday(date: datetime.date) -> bool:
    res = False
    holiday_list = get_belgian_holidays()

    if date in holiday_list:
        res = True
    
    return res


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
    dates_df = frame.select(colname).withColumn(new_colname, holiday_udf(frame[colname]))
    return dates_df.join(frame, on=colname, how="left")


def label_holidays3(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    """Adds a column indicating whether or not the column `colname`
    is a holiday. An alternative implementation.""" 
    return frame.withColumn(new_colname, col(colname).isin(get_belgian_holidays()))
