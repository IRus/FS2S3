# FS2S3
Sync local holder with S3.

## How to use:

1. [Create](https://console.aws.amazon.com/iam/home?#/security_credential) new Access Key.
2. Create file `env.sh`, see `env.sh.sample`.
3. Run sync.sh with two arguments: command and path.
    ```bash
    ./sync.sh pull ../folder  
    ```

Available commands:

* pull - will rewrite all files in directory.
* push - will rewrite all files in s3.
* list
