# TPC-DS PySpark

TPC-DS benchmark (3.2.0) on PySpark (Spark 3.2.4 with in-built Hadoop 2.7) using the DataFrames API.

A general summary of TPC-DS is [here](https://medium.com/hyrise/a-summary-of-tpc-ds-9fb5e7339a35)

## Setup

Taking Ubuntu as the operating system

Make sure the required development tools are installed:

```bash 
sudo apt-get install gcc make flex bison byacc git
```

## Generating Dataset

First compile in `tools` to generate `dsdgen`, which is used to generate dataset.

```bash
cd tools
make
# if the version needs gcc 9 
make CC=/usr/bin/gcc-9
# this should generate dsdgen and dsqgen
```

See `dsdgen -help` for all options. For example, `DIR` indicate where is the generated tables and `SCALE` option is used to indicate the volume of data to generate in GB. 

```bash
./dsdgen -SCALE 1 -DIR /home/<user_name>/dataset/tpcds
```

The above command will generate the dataset with around 1GB in the path `/home/<user_name>/dataset/tpcds`


## Generating Queries

The `dsqgen` is used to generate the queries for TPC-DS, see `dsqgen -help` for all options, but one example can be found below

```bash
./dsqgen \
-DIRECTORY ../query_templates \
-INPUT ../query_templates/templates.lst \ 
-VERBOSE Y \
-QUALIFY Y \
-SCALE 1 \
-DIALECT netezza \ 
-OUTPUT_DIR /home/<user_name>/tpcds/queries
```

However, there were two isseus for this command: 

1. There will be errors about substitution '_END'. To fix it, run `prepare-queries.sh` before generating queries
2. All the generated queries go to a single file, i.e., `query_0.sql`. To fix it, run `gen-individual-queries.sh` to generate individual queries.

## Running Queries

The running platform is PySpark, so we need to

1. Install PySpark which should match the version of installed Spark, e.g., PySpark 3.2.4 should match with Spark 3.2.4.

2. Run `tpcds_perf.py` by providing query id, dataset path, result path. It should be able to run all queries.

## Fixable failed queries

### Intervals like this '+ 14 days' not supported ###

Fix: use INTERVAL data type.

```bash
# fail
SELECT (now() + 14 days);

# success
SELECT (now() + INTERVAL 14 day);
```

### mismatched input '"xx_xx"' expecting {<EOF>, ';'}###

Fix: Change "xx_xx" to xx_xx.

```bash
# fail
count(distinct cs_order_number) as "order count"

# success
count(distinct cs_order_number) as ordercount
```

---
Reference:

TPC-DS, https://www.tpc.org/tpcds/default5.asp

