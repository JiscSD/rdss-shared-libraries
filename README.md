# RDSS Shared Libraries

Libraries used across RDSS components.

    * `kinesis` - a KinesisClient for applications that need to read from and write to Kinesis streams
    * `s3` - an S3Client for applications that need to write data to S3

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
