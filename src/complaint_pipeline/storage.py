"""Small local/GCS adapter. Bronze objects are published atomically, once."""

import json
import os
import tempfile
from pathlib import Path
from urllib.parse import urlsplit


def join(root, *parts):
    return "/".join([root.rstrip("/"), *parts])


class Store:
    def __init__(self, project=None):
        self.project = project
        self._client = None

    def _blob(self, uri):
        from google.cloud import storage

        if self._client is None:
            self._client = storage.Client(project=self.project)
        parsed = urlsplit(uri)
        return self._client.bucket(parsed.netloc).blob(parsed.path.lstrip("/"))

    def read(self, uri):
        if uri.startswith("gs://"):
            from google.api_core.exceptions import NotFound

            try:
                return self._blob(uri).download_as_bytes()
            except NotFound as exc:
                raise FileNotFoundError(uri) from exc
        return Path(uri).read_bytes()

    def put_once(self, uri, content):
        if uri.startswith("gs://"):
            from google.api_core.exceptions import PreconditionFailed

            try:
                self._blob(uri).upload_from_string(
                    content, content_type="application/json", if_generation_match=0
                )
            except PreconditionFailed as exc:
                raise FileExistsError(uri) from exc
            return
        path = Path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.link(tmp, path)  # Atomic create, never replace an existing capture.
        finally:
            os.unlink(tmp)

    def json(self, uri):
        return json.loads(self.read(uri))


def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False).encode("utf-8")
