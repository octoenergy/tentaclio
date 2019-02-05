[![CircleCI status](https://circleci.com/gh/octoenergy/data-io/tree/master.png?circle-token=df7aad11367f1ace5bce253b18efb6b21eaa65bc)](https://circleci.com/gh/octoenergy/data-io/tree/master)

# DataIO

Python package regrouping a collection of I/O connectors, used in the data world with the aim of providing:

- a boilerplate for developers to expose new connectors (`dataio.clients`).
- an interface to acess file resources,
    - thanks to a unified syntax (`dataio.open_reader`, `dataio.open_writer`),
    - and a simplified interface (`dataio.protocols`).

## Quickstart

Make sure [Homebrew](https://brew.sh/) is installed and ensure it's up to date.

    $ git clone git@github.com:octoenergy/data-io.git


### Local installation

Similarly to the [consumer-site](https://github.com/octoenergy/consumer-site/blob/master/README.md), the library must be deployed onto a machine running:

    - Python3
    - a C compiler (either `gcc` via Homebrew, or `xcode` via the App store)

Install [Pyenv](https://github.com/pyenv/pyenv) and [Pipenv](https://docs.pipenv.org/),

    $ brew install pyenv
    $ brew install pipenv

Lock the Python dependencies and build a virtualenv,

    $ make update

To refresh Python dependencies,

    $ make sync

## How to use
This is how to use `data-io` for your daily data ingestion and storing needs.

### URL streams
In order to open streams to load or store data the universal functions are:

```python
from dataio import open

with open("/path/to/my/file", 'r') as reader:
    contents = reader.read()
    [...]

with open("s3://bucket/file", 'w') as writer:
    writer.write(contents)
    [...]
```
The supported protocols are:

* `/local/file`
* `file:///local/file`
* `s3://bucket/file`
* `ftp://path/to/file`
* `sftp://path/to/file`
* `postgresql://host/database::table` will allow to write from a csv format into a database with the same column names (:warning: note that the table goes after `::` :warning:).

You can add the credentials for any of the urls in order to access protected resources.


You can use these readers and writers with pandas functions like:

```python
import pandas as pd
from dataio import open

with open("/path/to/my/file", 'r') as reader:
    df = pd.read_csv(reader) 

[...]

with open("s3::/path/to/my/file", 'w') as writer:
    df.to_parquet(writer) 
```
`Readers`, `Writers` and they closeable versions can be used anywhere expecting a file like object, pandas or pickle are examples of such functions.

### Automatic credentials injection. 
For urls opened through `dataio.open_reader` and `dataio.open_writer` dataio allows to automatically inject credentials without having to manipulate the urls.
In order for the credentials injection to work the corresponding _matching_ credentials have to be defined following the convention:

* environmental variables starting with the prefix `OCTOIO_CONN` i.e. `OCOTOIO_CONN_DATASETS_DATABASE_CREDENTIALS=postgresql://real_user:132ldsf@octoenergy.systems/datasets`
* The url passed to `dataio.open_reader` or `dataio.open_writer` has the correct scheme (`postgresql`), the correct correct host name, or the wildcard `hostname` if that should be injected too, 
and the correct path to identify matching credentials. In this case when calling `dataio.open_reader("postgresql://octoenergy.systems/datasets::my_table")` the correct credentials will be set for the 
client making the call equivalent to `dataio.open_reader("postgresql://real_user:132ldsf@octoenergy.systems/datasets::my_table")`. This is also equivalent to `dataio.open_reader("postgresql://hostname/datasets::my_table")`. Note that the `::table` part of the path doesn't need to be defined in the credentials url.

This is particulary useful when using dataio for distributed system where the url producer doesn't know the credentials of a given server, database, etc... and this information should not be travelling in plain text through the system.


### StreamClients and QueryClients

The module `dataio.client` gives more control over the resources, and allows to run queries against a database.  

```python
from dataio import clients 

[...] 

query = "select 1";
with clients.PostgresClient(POSTGRES_TEST_URL) as client:
    result =client.query(query)
[...]
```
     

## Development

### Testing

Tests run via `py.test`:

    $ make unit
    $ make integration

:warning: Unit and integration tests will require a `.env` in this directory with the following contents: :warning:

```dotenv
POSTGRES_TEST_URL=scheme://username:password@hostname:port/database
```

And linting taking care by `flake8` and `mypy`:

    $ make lint

### CircleCI

Continuous integration is run on [CircleCI](https://circleci.com/gh/octoenergy/workflows/data-io), with the following steps:

    $ make circleci

## Quick note on protocols
    
In order to abstract concrete dependencies from the implementation of data related functions (or in any part of the system really) we recommend to use [Protocols](https://mypy.readthedocs.io/en/latest/protocols.html#simple-user-defined-protocols). This allows a more flexible injection than using subclassing or [more complex approches](http://code.activestate.com/recipes/413268/). This idea is heavily inspired by how this exact thing is done in [go](https://www.youtube.com/watch?v=ifBUfIb7kdo).

### Simple protocol example

Let's suppose that we are going to write a function that loads a csv file, does some operation, and saves the result.

```python
import pandas as pd


def sum(input_file: str, output_file: str) -> None:
    df = pd.read_csv(input_file, index="index")
    transformed_df = _transform(df)
    pd.to_csv(output_file, transformed_df)
```

This has the following caveats:
* The source and destination of the data are bound to be a file in the local system, we can't support other streams such as s3, `io.StringIO`, or `io.BytesIO`.
* Testing is difficult and cumbersome as you need actual files for test the whole execution path.

Many panda's io functions allow the file argument (i.e. _input_file_) to be a string or a _buffer_, namely anything that contains a read method. This is known as a protocol in python.
Another protocols are `Sized`, any object that contains a `__len__` method, or `__getitem__`. Protocols are usually loosely bound to the receiver, and it's its respisability to check programmatically that the argument contains in fact that method.

We could refactor this piece of code using the classes from the `io` package to make it more general as they acutally implement the `read` protocol.

```python
import pandas as pd
from io import RawIOBase


def sum(input_file: RawIOBase, output_file: RawIOBase) -> None: # this won't work by the way
    df = pd.read_csv(input_file, index="index")
    transformed_df = _transform(df)
    pd.to_csv(output_file, transformed_df)
```

Now, the `io` package is a bit of a mess, it defines different classes such as `TextIOBase`, `StringIO`, `FileIO` which are similar but incompatible when it comes to typing due the differences between strings and bytes.  
If you want to use a `StringIO` or `FileIO` as argument to the same typed function, the whole process becomes an ordeal. Not only that, imagine that you want to implement a custom reader for data stored in a db, you will have to add a bunch of useless methods if you inherit from things like `IOBase`, as we are only interested in the _read_ method.

In order to have a more neat typed function that actually requires a read function we can use the [typing_extensions](https://pypi.org/project/typing-extensions/) package to create `Protocols`.

```python
from abc import abstractmethod
from typing_extensions import Protocol


class Reader(Protocol):

    @abstractmethod
    def read(self, i: int = -1):
        pass


class Writer(Protocol):

    @abstractmethod
    def write(self, content) -> int:
        pass
```
Notice how cheekily the methods above are not typed to allow strings and bytes to be sent in and out. 

Our function will look like something like this:

```python
import pandas as pd
from dataio import Reader, Writer


def sum(reader: Reader, writer: Writer) -> None:
    df = pd.read_csv(reader, index="index")
    transformed_df = _transform(df)
    pd.to_csv(writer, transformed_df)
```

In the new signature we force our input just to have a `read` method, likewise the output just needs a `write` method. 

Why is this cool? 
* Now we can accept anything that fulfills the protocol expected by pandas while we are checking its type.
* When creating new readers, we don't need to implement redundant methods to match any of the `io` base types.
* Testing becomes less cumbersome as we can send a `StringIO` rather than an actual file, or create some kind of fake class that has a `read` method. 

Caveats:
* the typing of the `pickle.dump` function is not consistent with its documentation and actual implementation, so you'll have to comment `# type: ignore` in order to use a `Writer` when calling `dump`.

## Pandas functions compatible with our Reader and Writer protocols 

Anything that expects a _filepath_or_buffer_. The full list of io functions for pandas is [here](https://pandas.pydata.org/pandas-docs/stable/io.html#io-sql), although they are not fully documented, i.e. parquet works even though it's not documented.
