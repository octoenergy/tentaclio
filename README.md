[![CircleCI status](https://circleci.com/gh/octoenergy/tentaclio/tree/master.png?circle-token=df7aad11367f1ace5bce253b18efb6b21eaa65bc)](https://circleci.com/gh/octoenergy/tentaclio/tree/master)


# Tentaclio

Python library that simplifies:
* Handling streams from different protocols such as `file:`, `ftp:`, `sftp:`, `s3:`, ...
* Opening database connections.
* Managing the credentials in distributed systems.

Main considerations in the design:
* Easy to use: all streams are open via `tentaclio.open`, all database connections through `tentaclio.db`.
* URLs are the basic resource locator and db connection string.
* Automagic authentication for protected resources.
* Extensible: you can add your own handlers for other schemes.
* Pandas interaction.

# Quick Examples.

## Read and write streams.
```python
import tentaclio
contents = "👋 🐙"

with tentaclio.open("ftp://localhost:2021/upload/file.txt", mode="w") as writer:
    writer.write(contents)

# Using boto3 authentication under the hood.
bucket = "s3://my-bucket/octopus/hello.txt"
with tentaclio.open(bucket) as reader:
    print(reader.read())
```

## Copy streams
```python
import tentaclio

tentaclio.copy("/home/constantine/data.csv", "sftp://constantine:tentacl3@sftp.octoenergy.com/uploads/data.csv")
```
## Delete resources
```python
import tentaclio

tentaclio.remove("s3://my-bucket/octopus/the-9th-tentacle.txt")
```
## List resources
```python
import tentaclio

for entry in tentaclio.listdir("s3:://mybucket/path/to/dir"):
    print("Entry", entry)
```

## Authenticated resources.
```python
import os

import tentaclio

print("env ftp credentials", os.getenv("OCTOIO__CONN__OCTOENERGY_FTP"))
# This prints `sftp://constantine:tentacl3@sftp.octoenergy.com/`

# Credentials get automatically injected.

with tentaclio.open("sftp://sftp.octoenergy.com/uploads/data.csv") as reader:
    print(reader.read())
```

## Database connections.
```python
import os

import tentaclio

print("env TENTACLIO__CONN__DB", os.getenv("TENTACLIO__CONN__DB"))

# This prints `postgresql://octopus:tentacle@localhost:5444/example`

# hostname is a wildcard, the credentials get injected.
with tentaclio.db("postgresql://hostname/example") as pg:
    results = pg.query("select * from my_table")
```

## Pandas interaction.
```python
import pandas as pd  # 🐼🐼
import tentaclio  # 🐙

df = pd.DataFrame([[1, 2, 3], [10, 20, 30]], columns=["col_1", "col_2", "col_3"])

bucket = "s3://my-bucket/data/pandas.csv"

with tentaclio.open(bucket, mode="w") as writer:  # supports more pandas readers
    df.to_csv(writer, index=False)

with tentaclio.open(bucket) as reader:
    new_df = pd.read_csv(reader)

# another example: using pandas.DataFrame.to_sql() with tentaclio to upload
with tentaclio.db(
        connection_info,
        connect_args={'options': '-csearch_path=schema_name'}
    ) as client:
    df.to_sql(
        name='observations', # table name
        con=client.conn,
    )
```

# Installation

You can get tentaclio using pip

```sh
pip install tentaclio
```
or pipenv
```sh
pipenv install tentaclio
```

## Developing.

Clone this repo and install [pipenv](https://pipenv.readthedocs.io/en/latest/):

In the `Makefile` you'll find some useful targets for linting, testing, etc. i.e.:
```sh
make test
```


## How to use
This is how to use `tentaclio` for your daily data ingestion and storing needs.

### Streams
In order to open streams to load or store data the universal function is:

```python
import tentaclio

with tentaclio.open("/path/to/my/file") as reader:
    contents = reader.read()

with tentaclio.open("s3://bucket/file", mode='w') as writer:
    writer.write(contents)

```
Allowed modes are `r`, `w`, `rb`, and `wb`. You can use `t` instead of `b` to indicate text streams, but that's the default.

In order to keep tentaclio as light as possible, it only includes `file`, `ftp`, `sftp`, `http` and `https` schemes by default.
However, many more are easily available by installing extra packages:

Default:
* `/local/file`
* `file:///local/file`
* `ftp://path/to/file`
* `sftp://path/to/file`
* `http://host.com/path/to/resource`
* `https://host.com/path/to/resource`

[tentaclio-s3](https://github.com/octoenergy/tentaclio-s3)
* `s3://bucket/file`

[tentaclio-gs](https://github.com/octoenergy/tentaclio-gs)
* `gs://bucket/file`
* `gsc://bucket/file`

[tentaclio-gdrive](https://github.com/octoenergy/tentaclio-gdrive)
* `gdrive:/My Drive/file`
* `googledrive:/My Drive/file`

[tentaclio-postgres](https://github.com/octoenergy/tentaclio-postgres)
* `postgresql://host/database::table` will allow you to write from a csv format into a database with the same column names (note that the table goes after `::` :warning:).


You can add the credentials for any of the urls in order to access protected resources.


You can use these readers and writers with pandas functions like:

```python
import pandas as pd
import tentaclio

with tentaclio.open("/path/to/my/file") as reader:
    df = pd.read_csv(reader)

[...]

with tentaclio.open("s3::/path/to/my/file", mode='w') as writer:
    df.to_parquet(writer)
```
`Readers`, `Writers` and their closeable versions can be used anywhere expecting a file-like object; pandas or pickle are examples of such functions.

### File system like operations to resources
#### Listing resources
Some URL schemes allow listing resources in a pythonnic way:
```python
import tentaclio

for entry in tentaclio.listdir("s3:://mybucket/path/to/dir"):
    print("Entry", entry)
```

Whereas `listdir` might be convinient we also offer `scandir`, which returns a list of [DirEntry](https://github.com/octoenergy/tentaclio/blob/ddbc28615de4b99106b956556db74a20e4761afe/src/tentaclio/fs/scanner.py#L13)s, and, `walk`. All functions follow as closely as possible their standard library definitions.


### Database access

In order to open db connections you can use `tentaclio.db` and have instant access to postgres, sqlite, athena and mssql.

```python
import tentaclio

[...]

query = "select 1";
with tentaclio.db(POSTGRES_TEST_URL) as client:
    result =client.query(query)
[...]
```

The supported db schemes are:

Default:
* `sqlite://`
* `mssql://`
* + Any other scheme supported by sqlalchemy.

[tentaclio-postgres](https://github.com/octoenergy/tentaclio-postgres)
* `postgresql://`

[tentaclio-athena](https://github.com/octoenergy/tentaclio-athena)
* `awsathena+rest://`

[tentaclio-databricks](https://github.com/octoenergy/tentaclio-databricks)
* `databricks+pyodbc://`


#### Extras for databases
For postgres you can set the variable `TENTACLIO__PG_APPLICATION_NAME` and the value will be injected
when connecting to the database.

### Automatic credentials injection

1. Configure credentials by using environmental variables prefixed with `TENTACLIO__CONN__`  (i.e.  `TENTACLIO__CONN__DATA_FTP=sfpt://real_user:132ldsf@ftp.octoenergy.com`).

2. Open a stream:
```python
with tentaclio.open("sftp://ftp.octoenergy.com/file.csv") as reader:
    reader.read()
```
The credentials get injected into the url.

3. Open a db client:
```python
import tentaclio

with tentaclio.db("postgresql://hostname/my_data_base") as client:
    client.query("select 1")
```
Note that `hostname` in the url to be authenticated is a wildcard that will match any hostname. So `authenticate("http://hostname/file.txt")` will be injected to `http://user:pass@octo.co/file.txt` if the credential for `http://user:pass@octo.co/` exists.

Different components of the URL are set differently:
- Scheme and path will be set from the URL, and null if missing.
- Username, password and hostname will be set from the stored credentials.
- Port will be set from the stored credentials if it exists, otherwise from the URL.
- Query will be set from the URL if it exists, otherwise from the stored credentials (so it can be
  overriden)

#### Credentials file

You can also set a credentials file that looks like:
```
secrets:
    db_1: postgresql://user1:pass1@myhost.com/database_1
    db_2: mssql://user2:pass2@otherhost.com/database_2?driver=ODBC+Driver+17+for+SQL+Server
    ftp_server: ftp://fuser:fpass@ftp.myhost.com
```
And make it accessible to tentaclio by setting the environmental variable `TENTACLIO__SECRETS_FILE`. The actual name of each url is for traceability and has no effect in the functionality.

(Note that you may need to add `?driver={driver from /usr/local/etc/odbcinst.ini}` for mssql database connection strings; see above example)

Alternatively you can run `curl https://raw.githubusercontent.com/octoenergy/tentaclio/master/extras/init_tentaclio.sh` to create a secrets file in `~/.tentaclio.yml` and
automatically configure your environment.

## Quick note on protocols structural subtyping.

In order to abstract concrete dependencies from the implementation of data related functions (or in any part of the system really) we use typed [protocols](https://mypy.readthedocs.io/en/latest/protocols.html#simple-user-defined-protocols). This allows a more flexible dependency injection than using subclassing or [more complex approches](http://code.activestate.com/recipes/413268/). This idea is heavily inspired by how this exact thing is done in [go](https://www.youtube.com/watch?v=ifBUfIb7kdo). Learn more about this principle in our [tech blog](https://tech.octopus.energy/news/2019/03/21/python-interfaces-a-la-go.html).
