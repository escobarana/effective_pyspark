# Building solid data pipelines with PySpark

📚 A course brought to you by the [Data Minded Academy].

## Context

These are the exercises used in the course *Building solid data pipelines with
PySpark*, reduced to the set that can be handled during our summer/winter
school format (that's a heavy reduction: from 12h of interactive moments to
about 5h).  The course has been developed by instructors at Data Minded. The
exercises are meant to be completed in the lexicographical order determined by
name of their parent folders. That is, exercises inside the folder `b_foo`
should be completed before those in `c_bar`, but both should come after those
of `a_foo_bar`.

## Getting started

While you can clone the repo locally, we do not offer support for setting up
your coding environment. Instead, we recommend you [tackle the exercises
using Gitpod][this gitpod].

[![Open in Gitpod][gitpod logo]][this gitpod]


⚠ IMPORTANT: After 30min of inactivity the gitpod environment shuts down and
you will lose unsaved progress.  If you want to save your work, either fork the
repo or create a branch (permissions needed). Don't forget to regularly sync
the upstream repo's changes in case of a fork, so that you can get the
solutions.

# Course objectives

- Introduce good data engineering practices.
- Illustrate modular and easily testable data transformation pipelines using
  PySpark.
- Illustrate PySpark concepts, like lazy evaluation, caching & partitioning.
  Not limited to these three though.

# Intended audience

- People working with (Py)Spark or soon to be working with it.
- Familiar with Python functions, variables and the container data types of
  `list`, `tuple`, `dict`, and `set`.

# Approach

Lecturer first sets the foundations right for Python development and
gradually builds up to PySpark data pipelines.

There is a high degree of participation expected from the students: they
will need to write code themselves and reason on topics, so that they can
better retain the knowledge. 
  
Participants are recommended to be working on a branch for any changes they
make, to avoid conflicts (otherwise the onus is on the participant), as the
instructors may choose to release an update to the current branch.

Note: this course is not about writing the best pipelines possible. There are
many ways to skin a cat, in this course we show one (or sometimes a few), which
should be suitable for the level of the participants.

## Exercises

### Adding derived columns

Check out [dates.py](exercises/c_labellers/dates.py) and implement the pure
python function `is_belgian_holiday`. verify your correct implementation by
running the test `test_pure_python_function` from
[test_labellers](tests/test_labellers.py). you could do this from the command
line with `pytest tests/test_labellers.py::test_pure_python_function`.

Return to [dates.py](exercises/c_labellers/dates.py) and
implement `label_weekend`. Again, run the related test from
[test_labellers.py](tests/test_labellers.py). It might be more useful to you if
you first read the test.

Finally, implement `label_holidays` from [dates](exercises/c_labellers/dates.py). 
As before, run the relevant test to verify a few easy cases (keep in mind that 
few tests are exhaustive: it's typically easier to prove something is wrong, 
than that something is right).

If you're making great speed, try to think of an alternative implementation 
to `label_holidays` and discuss pros and cons.

### Common business case 1: cleaning data

Using the information seen in the videos, prepare a sizeable dataset for 
storage in "the clean zone" of a data lake, by implementing the `clean` 
function of [clean_flights_starter.py](exercises/h_cleansers/clean_flights_starter.py).

### Peer review

In group, discuss the improvements one could make to 
[bingewatching.py](./exercises/l_code_review/bingewatching.py).

### Common business case 2: report generation

Create a complete view of the flights data in which you combine the airline
carriers (a dimension table), the airport names (another dimension table) and
the flights tables (a facts table).

Your manager wants to know how many flights were operated by American Airlines
in 2011.

How many of those flights arrived with less than (or equal to) 10 minutes of
delay?

A data scientist is looking for correlations between the departure delays and
the dates. In particular, he/she thinks that on Fridays there are relatively
speaking more flights departing with a delay than on any other day of the week.
Verify his/her claim.

Out of the 5 categories of sources for delays, which one appeared most often in
2011? In other words, in which category should we invest more time to improve?
applications

[this gitpod]: https://gitpod.io/#https://github.com/datamindedacademy/effective_pyspark
[gitpod logo]: https://gitpod.io/button/open-in-gitpod.svg
[Data Minded Academy]: https://www.dataminded.academy/
