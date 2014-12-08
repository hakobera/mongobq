# mongobq

Tool to import MongoDB collection into BigQuery

## Usage

```
  Usage: mongobq [options]

  Options:

    -h, --help                      output usage information
    -V, --version                   output the version number
    --host <hostname>               specifies a hostname for the mongod
    --port <port>                   specifies the TCP port for the mongod
    -u, --username <username>       specifies a mongodb username to authenticate
    -p, --password <password>       specifies a mongodb password to authenticate
    -d, --db <database>             specifies the name of the database
    -c, --collection <collection>   specifies the collection to export
    -f, --fields <field1[,field2]>  specifies a field or fields to include in the export
    -q, --query <JSON>              provides a JSON document as a query that optionally limits the documents returned in the export
    -K, --keyfile <keyfile>         specifies the key file path
    -B, --bucket <bucket>           specifies the Google Cloud Storage bucket name
    -O, --path <path>               specifies the output path of the bucket (Default: "/")
    -P, --project <project>         specifies the project ID of Google Cloud Platform
    -D, --dataset <dataset>         specifies the dataset ID of BigQuery
    -T, --table <table>             specifies the table ID of BigQuery
    -S, --schema <schemafile>       specifies the table schema of BigQuery table to import
    --autoclean                     clean after run
    --async                         No wait load job
    --dryrun                        Run as dryrun mode
```

## License

Apache License Version 2.0
