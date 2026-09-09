# dev/sftp — local SFTP test source

This folder is mounted into the `sftp` container at `/home/wprdc/outbound`, so
anything here is served by the local SFTP server (`localhost:2222`, user/pass
`wprdc`/`wprdc`).

Drop test files here and point a `component.yaml` source at them:

```yaml
source:
  type: sftp
  host: localhost
  port: 2222
  path: /outbound/*.csv
  secret_ref: WPRDC_LOCAL_SFTP   # export WPRDC_LOCAL_SFTP=wprdc:wprdc
```

`sample.csv` is a tiny generic file for smoke-testing the plumbing. To test a
real dataset end to end, drop a representative extract here — note that a
dataset with a full `schema.py` (all columns required) will fail its schema
check against a trimmed sample, which is expected.