# Fivetran Google Cloud Function connector

## Introduction

This program is used to mock Fivetran's engine requesting your Google Cloud function (or Cloud Run!).
The deamon manages the `hasMore` parameter and will return a request with the last state if this happen (to manage pagination).

## Fivetran documentation
https://www.fivetran.com/functions
https://fivetran.com/docs/functions/faq/use-hasmore-flag

## Cloud Function or Cloud Run

They actually both work.

## How to use it

### Send a request using Fivetran's setup convention

```
go run main.go --endpoint http://localhost:8080 --setup
# {"state": {}, "secrets": {}, "test_setup": true}
```

### Send a request locally

```
# localhost:8080 here
go run main.go \
    --endpoint http://localhost:8080 \
    --secrets '{"key1": "value1"}' \
    --state '{"key": "value"}'
```

### Send a request on Cloud Run
```
go run main.go \
    --endpoint https://your-url.a.run.app \
    --secrets '{"key1": "value1"}' \
    --state '{"key": "value"}' \
    --token "$(gcloud auth print-identity-token)"
```