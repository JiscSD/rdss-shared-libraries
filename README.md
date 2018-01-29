# RDSS Shared Libraries

Libraries used across RDSS components.

    * kinesis - a KinesisClient for applications that need to read from and write to Kinesis streams
    * taxonomy - a library to access local or Git versioned taxonomy schema mappings

## Developer setup

```
make env
source ./env/bin/activate
make deps
```

## Testing

```
make test
```
