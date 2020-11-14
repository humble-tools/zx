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
