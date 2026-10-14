"""Configuration and strict identifier validation for SQL rendering."""

import json
import re
import warnings
from dataclasses import dataclass, fields
from pathlib import Path


@dataclass(frozen=True)
class Config:
    project_name: str = "project_name"
    region: str = "asia-southeast1"
    bronze_root: str = "work/bronze"
    warehouse_root: str = "gs://project_name-warehouse"
    staging_dataset: str = "staging"
    silver_dataset: str = "silver"
    gold_dataset: str = "gold"
    ops_dataset: str = "ops"
    connection_id: str = "lakehouse_connection"
    api_url: str = "https://publicapi.traffy.in.th/share/teamchadchart/search"
    page_size: int = 100
    max_records: int = 1000
    start_offset: int = 0
    request_delay_seconds: float = 1.0
    timeout_seconds: int = 30
    max_attempts: int = 5
    max_rejected_fraction: float = 0.05
    max_bytes_billed: int = 10_000_000_000
    coordinate_order: str = "lon_lat"
    closed_states: tuple[str, ...] = ("เสร็จสิ้น",)
    open_states: tuple[str, ...] = ("รอรับเรื่อง", "กำลังดำเนินการ")

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if field.name.endswith("_dataset") or field.name == "connection_id":
                if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
                    raise ValueError(f"Unsafe identifier: {field.name}")
        if not re.fullmatch(r"[a-z][a-z0-9_-]*", self.project_name):
            raise ValueError("Invalid project_name")
        if not re.fullmatch(r"[a-z]+-[a-z]+[0-9]+", self.region):
            raise ValueError("Use a single GCP region, for example asia-southeast1")
        if not re.fullmatch(r"gs://[a-z0-9_-]+(?:/[A-Za-z0-9_/-]+)?", self.warehouse_root):
            raise ValueError("Invalid warehouse_root")
        if not self.api_url.startswith("https://") or "?" in self.api_url:
            raise ValueError("api_url must be HTTPS without query parameters")
        if not 1 <= self.page_size <= 1000 or not 1 <= self.max_records <= 100_000:
            raise ValueError("page_size must be 1..1000; max_records must be 1..100000")
        if self.start_offset < 0 or self.request_delay_seconds < 0:
            raise ValueError("offset and request delay must be nonnegative")
        if self.timeout_seconds <= 0 or self.max_attempts < 1:
            raise ValueError("timeouts and attempts must be positive")
        if not 0 <= self.max_rejected_fraction < 1 or self.max_bytes_billed <= 0:
            raise ValueError("Invalid quality or query cost limit")
        if self.coordinate_order not in {"lon_lat", "lat_lon"}:
            raise ValueError("coordinate_order must be lon_lat or lat_lon")
        if set(self.open_states) & set(self.closed_states):
            raise ValueError("open_states and closed_states must not overlap")

    def require_cloud(self):
        if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", self.project_name):
            raise ValueError("Replace project_name with your actual GCP project ID")
        for name in ("bronze_root", "warehouse_root"):
            value = getattr(self, name)
            if not value.startswith("gs://") or "project_name" in value:
                raise ValueError(f"Configure {name} with your own GCS location")

    @classmethod
    def from_dict(cls, values):
        # Existing ignored local configs may still contain the retired runner settings.
        retired = {
            "temp_root",
            "quarantine_root",
            "worker_service_account",
            "flex_template_uri",
            "subnetwork",
            "max_workers",
        }
        removed = retired.intersection(values)
        if removed:
            warnings.warn(
                "Ignored retired Dataflow settings: " + ", ".join(sorted(removed)),
                UserWarning,
                stacklevel=2,
            )
        return cls(**{key: value for key, value in values.items() if key not in retired})

    @classmethod
    def load(cls, path):
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))


def safe_run_id(value):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,99}", value):
        raise ValueError("run_id must be 1..100 letters, digits, underscores or hyphens")
    return value
