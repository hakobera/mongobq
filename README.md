# mongobq

Tool to import MongoDB collection into BigQuery

## How to use

```
  Usage: mongobq [options]

  Options:

    -h, --help                      output usage information
    -V, --version                   output the version number
    --host <hostname>               Specifies a hostname for the mongod
    --port <port>                   Specifies the TCP port for the mongod
    -u, --username <username>       Specifies a username with which to authenticate
    -p, --password <password>       Specifies a password with which to authenticate
    -d, --db <database>             Specifies the name of the database
    -c, --collection <collection>   Specifies the collection to export
    -f, --fields <field1[,field2]>  Specifies a field or fields to include in the export
    -q, --query <JSON>              Provides a JSON document as a query that optionally limits the documents returned in the export
    -k, --keyfile <keyfile>         Specifies the key file path
    --bucket <bucket>               Specifies the GCS bucket name
    --directory <path>              Specifies the GCS directory
    --project <project>             GCP project ID
    --dataset <dataset>             BigQuery dataset ID
    -t, --table <table>             BigQuery table name
    -s, --schema <schemafile>       Specifies the table schema of BigQuery table to import
    --autoclean                     Auto clearn after run
    --async                         No wait load job
    --dryrun                        Run as dryrun mode
    --verbose                       Show verbose log
```

## License

Apache License Version 2.0
