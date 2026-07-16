from dataclasses import dataclass, field
from typing import Optional
from uuid import uuid1


@dataclass
class SubprocessResult:
    id: str = field(default_factory=lambda: str(uuid1()).replace('-', ''))
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    returncode: Optional[int] = None
    killed_by_token: bool = False

    @property
    def success(self) -> bool:
        return self.returncode == 0

    @success.setter
    def success(self, _value: bool) -> None:
        raise AttributeError('The success property is read-only and cannot be assigned.')

    @success.deleter
    def success(self) -> None:
        raise AttributeError('The success property is read-only and cannot be deleted.')
