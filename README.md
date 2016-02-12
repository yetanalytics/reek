# reek

A (very) simple wrapper around [kria](https://github.com/bluemont/kria) for binary key value storage.

## Usage

Reek exposes a small subset of Riak's functionality for the specific use case of synchronously reading and writing arbitrary binary data (with string keys), leveraging secondary indexes (2i). You must use the levelDB backend for it to work. Only binary secondary indexes are supported.

## License

Copyright © 2016 Yet Analytics

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
