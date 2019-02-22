## How to use

`python testspl.py "/opt/hadoop/data/test.csv" "timechart span=1d max(val) as maxval, min(val) as minval" "./data/out.csv"`

## SPL Commands

* `where` (without `like`)
* `eval` (for math expressions only, without any functions)
* `timechart` (supported `span` and `as` statements)