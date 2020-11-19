# ZX

Column-based serverless database for observability.

- Separated storage and computing layers. Designed for serverless, cloud-native environment.
- Minimal maintenance required.
- Scale from local debugging to observing distributed systems.

## Usage

ZX supports a simple subset of SQL.

```
ZX.SQL <SQL QUERY>
```

ex. `ZX.SQL select count(repo.name) group by events`

## Development

ZX is a redis module. To build it:

1. Build ZX

```
$ cargo build
```

2. Load ZX as a redis module

```
redis-server --loadmodule ./target/debug/libxz.dylib
```

### Running tests

A simple dataset is provided in the `fixture/` directory. To load it:

```
$ redis-server --loadmodule <LIBXZ_PATH_RELATIVE_TO_DIR> --dir <FIXTURE_DIR> --dbfilename TS.rdb
```

For example, assuming you're in the project root:

```
$ redis-server --loadmodule ../target/debug/libzx.so --dir ./fixtures/ --dbfilename TS.rdb
```

In another console:

```
$ redis-cli ZX.T
"ok"
```
