# IMPLEMENTATION NOTES


The test is solved using 2 spark jobs (python):

daily_stats
-----------

This job Computes the stats for a day and stores the output as csv, in dated paths.
The job script computes the latest finished day by default, but it also accepts a date parameter which allows to process a specific day of a whole month.

repeaters
---------

This job computes the repeater customers and stores the output as csv.

This allows us to identify the repeaters:
customer_id: '7d1dd3c96c21c803f7a1a32aa8d9feb9' with 2 orders
customer_id: '9b8ce803689b3562defaad4613ef426f' with 2 orders


Each job includes shell scripts allowing them to be run by most schedulers.


# TODO:
- factorize jobs helpers (e.g.: read/write datasets)
- add tests with fake input data
- embed the dependencies in cluster mode
