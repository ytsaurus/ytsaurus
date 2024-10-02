from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .common import OrmContext
    from .model import OrmModel


class OrmClientPlugin:
    def post_initialize(self, context: "OrmContext") -> None:
        return

    def post_link(self, context: "OrmContext") -> None:
        return

    def post_finalize(self, model: "OrmModel") -> None:
        return
