from dataclasses import dataclass
from typing import ClassVar


@dataclass
class YtTabletState:
    MOUNTED: ClassVar[str] = "mounted"
    FROZEN: ClassVar[str] = "frozen"
    UNMOUNTED: ClassVar[str] = "unmounted"

    state: str | None = None

    def set(self, state: str | None):
        if state in (self.MOUNTED, self.FROZEN, self.UNMOUNTED):
            self.state = state
        else:
            self.state = None

    @property
    def is_not_set(self) -> bool:
        return not self.state

    @property
    def is_mounted(self) -> bool:
        return self.MOUNTED == self.state

    @property
    def is_unmounted(self) -> bool:
        return self.UNMOUNTED == self.state

    @property
    def is_frozen(self) -> bool:
        return self.FROZEN == self.state
