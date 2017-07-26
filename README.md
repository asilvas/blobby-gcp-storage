# blobby-s3

An GCP Storage client for [Blobby](https://github.com/asilvas/blobby), powered
by [Google Cloud Storage](https://github.com/googlecloudplatform/google-cloud-node#cloud-storage-ga).



## Options

```
# config/local.json5
{
  storage: {
    app: {
      options: {
        project: 'myproject-982334',
        bucket: 'mybucket-12353.appspot.com'
      }
    }
  }
}
```

| Option | Type | Default | Desc |
| --- | --- | --- | --- |
| project | string | (required) | Project identifier for the storage |
| bucket | string | (required) | Bucket within the storage |


### Authentication

Automatically uses the default Google Cloud Authentication. No special steps.
