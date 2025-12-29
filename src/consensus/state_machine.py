from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class InMemoryStateMachine:
    applied: List[Dict[str, Any]] = field(default_factory=list)

    def apply(self, cmd: Dict[str, Any]) -> None:
        self.applied.append(cmd)

    def snapshot(self) -> Dict[str, Any]:
        return {"applied_len": len(self.applied), "last": self.applied[-1] if self.applied else None}
