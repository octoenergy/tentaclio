[![CircleCI status](https://circleci.com/gh/octoenergy/data-io/tree/master.png?circle-token=df7aad11367f1ace5bce253b18efb6b21eaa65bc)](https://circleci.com/gh/octoenergy/data-io/tree/master)

# data-io
Single repository regrouping all IO connectors used in the data world.


## How to use protocols.
    
In order to abstract concrete dependencies from the implementation of data related functions (or in any part of the system really) we recommend to use [Protocols](https://mypy.readthedocs.io/en/latest/protocols.html#simple-user-defined-protocols). This allows a more flexible injection than using subclassing or [more complex approches](http://code.activestate.com/recipes/413268/). This idea is heavily inspired by how this exact thing is done in [go](https://www.youtube.com/watch?v=ifBUfIb7kdo).

### Simple protocol example.

Let's suppose that we are going to write a function that loads a csv file, does some operation, and saves the result.

```python
import pandas as pd
def sum(input_file: str, output_file: str) -> :
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

def sum(input_file: RawIOBase, output_file: RawIOBase): # this won't work by the way
    df = pd.read_csv(input_file, index="index")
    transformed_df = _transform(df)
    pd.to_csv(output_file, transformed_df)
```

Now, the `io` package is a bit of a mess, it defines different classes such as `TextIOBase`, `StringIO`, `FileIO` which are similar but incompatible when it comes to typing due the differences between strings and bytes.  
If you want to use a `StringIO` or `FileIO` as argument to the same typed function, the whole process becomes an ordeal. Not only that, imagine that you want to implement a custom reader for data stored in a db, you will have to add a bunch of useless methods if you inherit from things like `IOBase`, as we are only interested in the _read_ method.

In order to have a more neat typed function that actually requires a read function we can use the [typing_extensions](https://pypi.org/project/typing-extensions/) package to create `Protocols`.

```python
from abc import abstractmethod

from typing_extensions import Protocols

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

def sum(reader: Reader, writer: Writer) -> :
    df = pd.read_csv(reader, index="index")
    transformed_df = _transform(df)
    pd.to_csv(writer, transformed_df)

```

In the new signature we force our input just to have a `read` method, likewise the output just needs a `write` method. 

Why is this cool? 
* Now we can accept anything that fulfills the protocol expected by pandas while we are checking its type.
* When creating new readers, we don't need to implement redundant methods to match any of the `io` base types.
* Testing becomes less cumbersome as we can send a `StringIO` rather than an actual file, or create some kind of fake class that has a `read` mehtod. 

Caveats:
* the typing of the `pickle.dump` function is not consistent with its documentation and actual implementation, so you'll have to comment `# type: ignore` in order to use a `Writer` when calling `dump`.

## Pandas functions compatible with our Reader and Writer protocols. 

Anything that expects a _filepath_or_buffer_. The full list of io functions for pandas is [here](https://pandas.pydata.org/pandas-docs/stable/io.html#io-sql), althogh they are not fully documented, i.e. parquet works eventhough it's not documented.


