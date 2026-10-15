"""
Data models conforming to Section 2, 3, 4, and 6 of the MayaCardio assessment.
"""

from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Union, Any
import json


@dataclass
class Sample:
    sensor: int
    index: int
    values: List[int]
    session_id: Optional[str] = None

    def to_export_dict(self) -> Dict[str, Any]:
        """Export format required by Section 4: {"sensor": 1, "index": 120000, "values": [720, 1200, 1560]}"""
        return {
            "sensor": self.sensor,
            "index": self.index,
            "values": self.values
        }

    def to_export_ndjson(self) -> str:
        return json.dumps(self.to_export_dict(), separators=(", ", ": "))


@dataclass
class LossRecord:
    sensor: int
    from_index: int
    to_index: Optional[int]
    reason: str
    session_id: Optional[str] = None

    def to_export_dict(self) -> Dict[str, Any]:
        """Export format required by Section 4: {"sensor": 2, "from_index": 4000, "to_index": 4399, "reason": "..."}"""
        return {
            "sensor": self.sensor,
            "from_index": self.from_index,
            "to_index": self.to_index,
            "reason": self.reason
        }

    def to_export_ndjson(self) -> str:
        return json.dumps(self.to_export_dict(), separators=(", ", ": "))


@dataclass
class WindowResult:
    session_id: str
    window: int
    missing_1: int
    missing_2: int
    complete: bool
    mean: Optional[Dict[str, Optional[List[float]]]]
    min: Optional[Dict[str, Optional[List[int]]]]
    max: Optional[Dict[str, Optional[List[int]]]]
    magnitude: Optional[float]
    heart_rate: Optional[float]
    mode: str  # "online" or "offline"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "window": self.window,
            "missing_1": self.missing_1,
            "missing_2": self.missing_2,
            "complete": self.complete,
            "mean": self.mean,
            "min": self.min,
            "max": self.max,
            "magnitude": self.magnitude,
            "heart_rate": self.heart_rate,
            "mode": self.mode
        }

    def to_ndjson(self) -> str:
        return json.dumps(self.to_dict(), separators=(", ", ": "))


@dataclass
class PipelineStatus:
    mode: str
    samples_saved: int
    samples_cloud: int
    backlog: int
    session_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "mode": self.mode,
            "samples_saved": self.samples_saved,
            "samples_cloud": self.samples_cloud,
            "backlog": self.backlog
        }
        if self.session_id:
            d["session_id"] = self.session_id
        return d

    def to_ndjson(self) -> str:
        return json.dumps(self.to_dict(), separators=(", ", ": "))
