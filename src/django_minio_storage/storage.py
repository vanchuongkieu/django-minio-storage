from io import BytesIO
from tempfile import TemporaryFile
from urllib.parse import urlparse

from django.core.files.uploadedfile import InMemoryUploadedFile

from django.conf import settings
from django.core.files.base import File
from django.core.files.storage import Storage
from django.utils.deconstruct import deconstructible
from minio import Minio


@deconstructible
class MinioStorage(Storage):
    bucket_name = getattr(settings, "MINIO_BUCKET_NAME", None)
    _protocol = "http"
    _base_url = None

    def get_endpoint(self, endpoint: str | None) -> str | None:
        if not endpoint:
            return None

        endpoint = endpoint.strip()

        if endpoint.startswith(("http://", "https://")):
            parsed = urlparse(endpoint)
            return parsed.netloc.rstrip("/")

        return endpoint.rstrip("/")

    def __init__(
        self,
        bucket_name=None,
        endpoint=None,
        access_key=None,
        secret_key=None,
        secure=True,
    ):
        overrides = locals()
        defaults = {
            "endpoint": getattr(settings, "MINIO_ENDPOINT", None),
            "access_key": getattr(settings, "MINIO_ACCESS_KEY", None),
            "secret_key": getattr(settings, "MINIO_SECRET_KEY", None),
            "secure": getattr(settings, "MINIO_SECURE", False),
        }
        self.bucket_name = bucket_name or overrides["bucket_name"] or self.bucket_name
        kwargs = {k: overrides[k] or v for k, v in defaults.items()}
        kwargs["endpoint"] = self.get_endpoint(kwargs["endpoint"])

        requires = ["endpoint", "access_key", "secret_key"]

        validates = [kwargs.get(param) is not None for param in requires]
        validates.append(self.bucket_name is not None)

        if not all(validates):
            raise ValueError(f"Minio requires {', '.join(requires)} parameters.")

        self._protocol = "https" if kwargs["secure"] else "http"
        self._base_url = f"{self._protocol}://{kwargs['endpoint']}"
        self.minio = Minio(**kwargs)

    def _upload_file(self, object_name: str | None, file_data: InMemoryUploadedFile):
        content_type = getattr(file_data, "content_type", "application/octet-stream")
        self.minio.put_object(
            bucket_name=self.bucket_name,
            object_name=object_name,
            data=file_data.file,
            length=file_data.size,
            content_type=content_type,
        )
        return object_name

    def save(self, object_name, object_content, max_length=None):
        return self._upload_file(object_name, object_content)

    def delete(self, object_name):
        self.minio.remove_object(object_name=object_name, bucket_name=self.bucket_name)

    def exists(self, object_name):
        return False

    def _temporary_storage(self, contents):
        conent_file = TemporaryFile(contents, "r+")
        return conent_file

    def open(self, object_name, mode="rb"):
        response = self.minio.get_object(self.bucket_name, object_name)
        try:
            object_bytes = response.read()
            output = BytesIO()
            output.write(object_bytes)
            output.seek(0)
            return File(output, object_name)
        finally:
            response.close()
            response.release_conn()

    def url(self, object_name):
        return "{}/{}/{}".format(self._base_url, self.bucket_name, object_name)
