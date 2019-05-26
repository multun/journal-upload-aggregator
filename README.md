# What is this thing ?

An alternative to systemd-journal-remote, inserting logs into elasticsearch.

It aggregates logs from systemd-journal-upload.

```
usage: journal-upload-aggregator [-h] [-v] [-q] [--host HOST]
                                 [--watchdog-timeout WATCHDOG_TIMEOUT]
                                 [--batch-size BATCH_SIZE] [-p PORT]
                                 elastic-url index-name

Start a systemd to elasticsearch journal aggregator

positional arguments:
  elastic-url           Url to send batched logs to
  index-name            Name of the elasticsearch index

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         The more v's, the more logs
  -q, --quiet           The more q's, the less logs
  --host HOST           Hostname to listen to
  --watchdog-timeout WATCHDOG_TIMEOUT
                        Interval at which a watchdog should check for old
                        incomplete batches
  --batch-size BATCH_SIZE
                        How big does a batch needs to be before being sent
  -p PORT, --port PORT  Port to listen to
```

# How to configure `systemd-journal-upload` ?

`/lib/systemd/systemd-journal-upload --url=http://whatever/gateway`

# Monitoring

Prometheus metrics can be gathered from `/health`.
