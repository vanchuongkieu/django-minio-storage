# Django MinIO Storage

Django Storage backend using MinIO.

## Install

```bash
pip install django-minio-storage
```

## Usage

```python
# settings.py
MINIO_BUCKET_NAME = ""
MINIO_ENDPOINT = "" # s3.example.com
MINIO_ACCESS_KEY = ""
MINIO_SECRET_KEY = ""
MINIO_SECURE = False  # True or False

# For Django 4.2+
STORAGES = {
    "default": {
        "BACKEND": "django_minio_storage.MinioStorage",
    },
}

# For Django versions below 4.2
DEFAULT_FILE_STORAGE = "django_minio_storage.MinioStorage"
```