# DataFusion-CatalogProvider-Glue

[Glue](https://aws.amazon.com/glue) as a CatalogProvider for [Datafusion](https://github.com/apache/arrow-datafusion).

Output from [demo](./examples/demo.rs) example:
```text
mbpro16(timvw)➜  datafusion-catalogprovider-glue git:(main) ✗ cargo run --example=demo

   Compiling datafusion-catalogprovider-glue v0.1.0 (/Users/timvw/src/github/datafusion-catalogprovider-glue)
    Finished dev [unoptimized + debuginfo] target(s) in 7.43s
     Running `target/debug/examples/demo`
registering tpc-h-parquet-1.customer
+---------------+--------------------+------------+------------+
| table_catalog | table_schema       | table_name | table_type |
+---------------+--------------------+------------+------------+
| glue          | tpc-h-parquet-1    | customer   | BASE TABLE |
| glue          | information_schema | tables     | VIEW       |
| glue          | information_schema | columns    | VIEW       |
| datafusion    | information_schema | tables     | VIEW       |
| datafusion    | information_schema | columns    | VIEW       |
+---------------+--------------------+------------+------------+
+---------------+-----------------+------------+--------------+------------------+----------------+-------------+-----------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+
| table_catalog | table_schema    | table_name | column_name  | ordinal_position | column_default | is_nullable | data_type | character_maximum_length | character_octet_length | numeric_precision | numeric_precision_radix | numeric_scale | datetime_precision | interval_type |
+---------------+-----------------+------------+--------------+------------------+----------------+-------------+-----------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+
| glue          | tpc-h-parquet-1 | customer   | c_custkey    | 0                |                | NO          | Int64     |                          |                        |                   |                         |               |                    |               |
| glue          | tpc-h-parquet-1 | customer   | c_name       | 1                |                | YES         | Utf8      |                          | 2147483647             |                   |                         |               |                    |               |
| glue          | tpc-h-parquet-1 | customer   | c_address    | 2                |                | YES         | Utf8      |                          | 2147483647             |                   |                         |               |                    |               |
| glue          | tpc-h-parquet-1 | customer   | c_nationkey  | 3                |                | NO          | Int64     |                          |                        |                   |                         |               |                    |               |
| glue          | tpc-h-parquet-1 | customer   | c_phone      | 4                |                | YES         | Utf8      |                          | 2147483647             |                   |                         |               |                    |               |
| glue          | tpc-h-parquet-1 | customer   | c_acctbal    | 5                |                | NO          | Float64   |                          |                        | 24                | 2                       |               |                    |               |
| glue          | tpc-h-parquet-1 | customer   | c_mktsegment | 6                |                | YES         | Utf8      |                          | 2147483647             |                   |                         |               |                    |               |
| glue          | tpc-h-parquet-1 | customer   | c_comment    | 7                |                | YES         | Utf8      |                          | 2147483647             |                   |                         |               |                    |               |
+---------------+-----------------+------------+--------------+------------------+----------------+-------------+-----------+--------------------------+------------------------+-------------------+-------------------------+---------------+--------------------+---------------+
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------------------+
| c_custkey | c_name             | c_address                             | c_nationkey | c_phone         | c_acctbal | c_mktsegment | c_comment                                                                                                         |
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------------------+
| 1         | Customer#000000001 | IVhzIApeRb ot,c,E                     | 15          | 25-989-741-2988 | 711.56    | BUILDING     | to the even, regular platelets. regular, ironic epitaphs nag e                                                    |
| 2         | Customer#000000002 | XSTf4,NCwDVaWNe6tEgvwfmRchLXak        | 13          | 23-768-687-3665 | 121.65    | AUTOMOBILE   | l accounts. blithely ironic theodolites integrate boldly: caref                                                   |
| 3         | Customer#000000003 | MG9kdTD2WBHm                          | 1           | 11-719-748-3364 | 7498.12   | AUTOMOBILE   | deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov             |
| 4         | Customer#000000004 | XxVSJsLAGtn                           | 4           | 14-128-190-5944 | 2866.83   | MACHINERY    | requests. final, regular ideas sleep final accou                                                                  |
| 5         | Customer#000000005 | KvpyuHCplrB84WgAiGV6sYpZq7Tj          | 3           | 13-750-942-6364 | 794.47    | HOUSEHOLD    | n accounts will have to unwind. foxes cajole accor                                                                |
| 6         | Customer#000000006 | sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn  | 20          | 30-114-968-4951 | 7638.57   | AUTOMOBILE   | tions. even deposits boost according to the slyly bold packages. final accounts cajole requests. furious          |
| 7         | Customer#000000007 | TcGe5gaZNgVePxU5kRrvXBfkasDTea        | 18          | 28-190-982-9759 | 9561.95   | AUTOMOBILE   | ainst the ironic, express theodolites. express, even pinto beans among the exp                                    |
| 8         | Customer#000000008 | I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5 | 17          | 27-147-574-9335 | 6819.74   | BUILDING     | among the slyly regular theodolites kindle blithely courts. carefully even theodolites haggle slyly along the ide |
| 9         | Customer#000000009 | xKiAFTjUsCuxfeleNqefumTrjS            | 8           | 18-338-906-3675 | 8324.07   | FURNITURE    | r theodolites according to the requests wake thinly excuses: pending requests haggle furiousl                     |
| 10        | Customer#000000010 | 6LrEaV6KR6PLVcgl2ArL Q3rqzLzcT1 v2    | 5           | 15-741-346-9870 | 2753.54   | HOUSEHOLD    | es regular deposits haggle. fur                                                                                   |
+-----------+--------------------+---------------------------------------+-------------+-----------------+-----------+--------------+-------------------------------------------------------------------------------------------------------------------+

```

## Development

Standard rust toolchain, eg:

```bash
cargo build
cargo test
```

Run all linting:

```bash
./dev/rust_lint.sh
```

## Testing

First clone the test data repository:

```bash
git submodule update --init --recursive
```

When this does not work:

```bash
git submodule add -f https://github.com/apache/parquet-testing.git parquet-testing
git submodule add -f https://github.com/apache/arrow-testing testing
```

Upload the testdata:

```bash
aws s3api create-bucket \
    --bucket datafusion-testing \
    --region eu-central-1 \
    --create-bucket-configuration LocationConstraint=eu-central-1

find testing  -type f -exec aws s3 cp ./{} s3://datafusion-{} \;

aws s3api create-bucket \
    --bucket datafusion-parquet-testing \
    --region eu-central-1 \
    --create-bucket-configuration LocationConstraint=eu-central-1

find parquet-testing  -type f -exec aws s3 cp ./{} s3://datafusion-{} \;
```

Create Glue databases:

```bash
aws glue create-database \
    --database-input "{\"Name\":\"datafusion\"}"

```


