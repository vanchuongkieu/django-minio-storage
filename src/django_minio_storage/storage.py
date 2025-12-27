from io import BytesIO
from tempfile import NamedTemporaryFile
from typing import BinaryIO, Optional
from urllib.parse import urlparse

from django.core.files.base import File
from django.core.files.storage import Storage
from django.utils.deconstruct import deconstructible
from minio import Minio
from minio.error import S3Error


@deconstructible
class MinioStorage(Storage):
    """Django Storage backend for MinIO.

    Reads configuration from Django settings when not provided explicitly:
    - STORAGE_MINIO_ENDPOINT
    - STORAGE_MINIO_ACCESS_KEY
    - STORAGE_MINIO_SECRET_KEY
    - STORAGE_MINIO_SECURE
    - STORAGE_MINIO_BUCKET_NAME

    # For Django 4.2+
    STORAGES = {
        "default": {
            "BACKEND": "django_minio_storage.MinioStorage",
        },
    }

    # Or with options
    STORAGES = {
        "default": {
            "BACKEND": "django_minio_storage.MinioStorage",
            "OPTIONS": {
                "bucket_name": "",
                "endpoint": "", # s3.example.com
                "access_key": "",
                "secret_key": "",
                "secure": True,  # True or False
            },
        },
    }

    # For Django versions below 4.2
    DEFAULT_FILE_STORAGE = "django_minio_storage.MinioStorage"
    """

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        secure: Optional[bool] = None,
    ):
        from django.conf import settings

        endpoint = endpoint or getattr(settings, "STORAGE_MINIO_ENDPOINT", None)
        access_key = access_key or getattr(settings, "STORAGE_MINIO_ACCESS_KEY", None)
        secret_key = secret_key or getattr(settings, "STORAGE_MINIO_SECRET_KEY", None)
        bucket_name = bucket_name or getattr(settings, "STORAGE_MINIO_BUCKET_NAME", None)

        if secure is None:
            secure = getattr(settings, "STORAGE_MINIO_SECURE", False)

        endpoint = self._normalize_endpoint(endpoint)

        if not all([endpoint, access_key, secret_key, bucket_name]):
            raise ValueError("Minio requires endpoint, access_key, secret_key and bucket_name.")

        self.bucket_name = bucket_name
        self._protocol = "https" if secure else "http"
        self._base_url = f"{self._protocol}://{endpoint}"
        self.minio = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    @staticmethod
    def _normalize_endpoint(endpoint: Optional[str]) -> Optional[str]:
        if not endpoint:
            return None
        endpoint = endpoint.strip()
        if endpoint.startswith(("http://", "https://")):
            parsed = urlparse(endpoint)
            return parsed.netloc.rstrip("/")
        return endpoint.rstrip("/")

    def _get_file_obj_and_size(self, content) -> tuple[BinaryIO, int]:
        file_obj = getattr(content, "file", None) or getattr(content, "stream", None) or content

        size = getattr(content, "size", None)
        if size is None:
            buf = BytesIO()
            try:
                file_obj.seek(0)
            except Exception:
                pass
            buf.write(file_obj.read())
            buf.seek(0)
            file_obj = buf
            size = buf.getbuffer().nbytes

        return file_obj, int(size)

    def save(self, name: str, content, max_length=None):
        file_obj, size = self._get_file_obj_and_size(content)
        content_type = getattr(content, "content_type", "application/octet-stream")

        try:
            file_obj.seek(0)
        except Exception:
            pass

        self.minio.put_object(
            bucket_name=self.bucket_name,
            object_name=name,
            data=file_obj,
            length=size,
            content_type=content_type,
        )

        return name

    def open(self, name: str, mode: str = "rb"):
        response = self.minio.get_object(self.bucket_name, name)
        try:
            data = response.read()
            buf = BytesIO(data)
            buf.seek(0)
            return File(buf, name)
        finally:
            response.close()
            response.release_conn()

    def exists(self, name: str) -> bool:
        try:
            self.minio.stat_object(self.bucket_name, name)
            return True
        except S3Error:
            return False

    def delete(self, name: str) -> None:
        try:
            self.minio.remove_object(bucket_name=self.bucket_name, object_name=name)
        except S3Error:
            pass

    def url(self, name: str) -> str:
        return f"{self._base_url}/{self.bucket_name}/{name}"

    def _temporary_storage(self, contents=None):
        return NamedTemporaryFile(mode="w+b", delete=True)
