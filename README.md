# Ikea  assignment


The task is described at the  `IKEA Data Engineering assignment Inspirational Feed.pdf`,
which can be found in the root of the repo.


# Code remarks

The provided solution allows to process the dataset in tow ways, using `Pyspark`  as an engine.
The first solution aims to process a given data file, while the second one is a streaming solution.
The streaming solution keeps track of the `STREAM_DATA` folder, and as soon as new data arrives -- performs transformation.

The class for streaming extends the class for non-streaming solution, `class StreamPipeline(BasicPipeline)`, as an example of how one can extend the solution.

Please keep in mind, that the provided solution has its pitfalls, like i do not catch any read-write exceptions etc., but that is solely due to the specifics of the project (it is a  test/dev, not  production) and lack of time (the suggested 2  hours are  already way behind).


# Bonus questions

`What if we need to built-up a live dashboard on it, would you change your data model?`

-------

When building dashboard solution i usually use `Grafana`, and the underlying DB as `InfluxDB`.
Python has good support  forr the InfluxDB. Here is the sketch which i took from one of my projects:

```python
from influxdb import  DataFrameClient
DB_NAME = args.db
client = DataFrameClient(host='127.0.0.1', port=8086, username='***', password='***', database=DB_NAME)


def update_db(fname):
    """
    updateds the DB if there are new dates in the Parquet file
    """
    metrics_pd = load_metrics(fname)
    logger.info("metrics data collected")

    logger.info('writing measurements to the InfuxDB')
    for metric in metrics_pd.columns:
        client.write_points(metrics_pd[[metric]], metric)
        logger.info("metric %s written" %metric)
```
`load_metrics` -- grabs the data from Parquet file, and populates the DB.
On Grafans side -- just pick up proper DB and display the data needed.

-------
`Assume it’s a streaming scenario, what would you change, and what technology you would use? (brief
architecture description).`

------
This actually been done. Please  refer to the docs and test it.

-----
`How would you apply a CI/CD on this code?`

-----
1. put  the code in the remote (github, bitbucket)
1. configure software (gitlab, TeamCity, etc.)
1. setup triggers, schedule
1. push the code to the remote
1. CI/CD will run tests, deploy



# The  repo
The repo is a bit exhaustive, suitable more for the real package for PyPi,
rather than for a  short assignment.
So it contains by default a number of files, which I will not explain here.



# Repo structure

```bash
 ❯ tree -L  2                                                                                                                               [11:13:34]
.
├── AUTHORS.rst
├── CONTRIBUTING.rst
├── HISTORY.rst
├── IKEA\ Data\ Engineering\ assignment\ Inspirational\ Feed.pdf
├── MANIFEST.in
├── Makefile
├── README.md
├── data
│   ├── input
│   ├── input_stream
│   └── output
├── docs
├── ikea
├── ikea_assignment
│   ├── __init__.py
│   ├── ikea_assignment.py
│   └── queries.py
├── log
├── requirements.txt
├── requirements_dev.txt
├── setup.cfg
├── setup.py
├── tests
│   ├── __init__.py
│   └── test_ikea_assignment.py
└── tox.ini
```


The folder `ikea_assignment`  is  the package folder, `ikea_assignment.py` -- the main file, `queries.py` -- file for holding queries from the task:

```bash
├── ikea_assignment
│   ├── __init__.py
│   ├── ikea_assignment.py
│   └── queries.py
```

and  the tests folder:

```bash
tests
├── __init__.py
└── test_ikea_assignment.py
```



the `data` folder keeping the  data :
```bash
├── data
│   ├── input            #holds raw data
│   ├── input_stream      # initially empty, put there raw datafile while running streaming
│   └── output          # place for output in Parquet format
```



# Docs & Logs

Due to the shortage of time and size of the project, there is no docs and logs, but all the code  contains the properly formatted doc-strings,  so tools like `Sphinx` will generate the docs automatically.

The same with  logging and command prompt arguments... For bigger  projects I use standard `logging`  and `argparse` modules correspondingly.




# How to launch

1. unzip the content of the archive:` ❯ tar -zxvf archive.tar.gz`. This creates a `ikea_assignment/` folder.
1. go to that  folder and issue: `python3 -m venv ikea` to create a virtual environment  for the Python.
1. issue: `source ikea/bin/activate` to activate the environment. Make sure you are in  the `venv` by checking `which python`. That must point  to the `ikea/bin/python`:
```bash
❯ which python
/Users/alexey/Documents/projects/ikea_assignment/ikea/bin/python
```
1. issue the
```bash
❯ pip3 install -r requirements.txt
```
to install the needed libraries.

1. if all is good, run tests as:
```bash
❯ pytest -v tests
====================test session starts ==========================
platform darwin -- Python 3.9.1, pytest-6.2.2, py-1.10.0, pluggy-0.13.1
-- /Users/alexey/Documents/projects/ikea_assignment/ikea/bin/python3
cachedir: .pytest_cache
rootdir: /Users/alexey/Documents/projects/ikea_assignment, configfile: setup.cfg
collected 3 items
tests/test_ikea_assignment.py::TestBasic::test_line_count_raw PASSED                                                    [ 33%]
tests/test_ikea_assignment.py::TestBasic::test_line_count_processed_dataset PASSED                           [ 66%]
tests/test_ikea_assignment.py::TestBasic::test_number_of_items_by_two_methods PASSED                  [100%]
============================== 3 passed in 10.75s ====================
```
1. to run the transformation pipeline (and the needed queries) use:
```bash
❯ python ikea_assignment/ikea_assignment.py
```
It will produce output  to the console, where you should  look for the headers:
```bash
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
the most sold products
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
day for the highest revenue
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
top 5 selling products for each day
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
```
Those contain the output for the queries from the assignment in both PySpark and SQL syntax.
 **Just in case the output  is saved to the `log` file, located in the root of the repo.**
1.  To test the streaming solution do the following:
    1. uncomment the
    ``` bash
      # stream = StreamPipeline(STREAM_DATA, spark)
    # df = stream.transform()
    # stream.save()
    ```
    in the bottom of `ikea_assignment.py`.
    1. make sure to clean the
    ```python
    OUTPUT = "data/output/data.parquet"
STREAM_DATA = 'data/input_stream'
checkpointLocation = "file:///tmp/stream/checkpoint"
    ```
    locations, like
    ```bash
    rm -rf data/output/data.parquet
    rm -rf data/input_stream
    rm -rf /tmp/stream/checkpoint
    ```
    1. launch the :
    ```bash
❯ python ikea_assignment/ikea_assignment.py
```
    1. as soon as you will see the prompt:
    ```bash
    saving the stream. It will wait 60 seconds and terminates...
        Put the  `dataset.jsonl` in the  `data_stream` directory.,
    ```
    copy the `dataset.jsonl` to the `data/input_stream` folder. You have 1 minute to do so. After that,  the stream will be terminated, and the result  can be checked at `data/output/data.parquet`.

# Soft  used

1. `Pyspark` as the engine for distributed calculations.
1. `Python`  as the  programming language.
1. `Parquet` format for processed data storage, which delivers very good performance and scalability (HDFS needed)
