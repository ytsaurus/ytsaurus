from .taggable import SystemFields
from .sensor import Sensor, Text, EmptyCell


class SerializerBase():
    def on_cell_content(self, sensor):
        raise NotImplementedError()

    def on_cell(self, cell, content):
        return content

    def on_row(self, row, cells):
        return cells

    def on_rowset(self, rowset, rows):
        return rows

    def on_dashboard(self, dashboard, rowsets):
        rows = []
        for rowset in rowsets:
            rows += rowset
        return rows

    def on_options(self, options):
        pass


class DebugTagPostprocessor():
    def postprocess(self, tags, sensor_name=None):
        return (self._unpack_system_tags(tags), sensor_name)

    def _unpack_system_tags(self, tags):
        res = []
        for k, v in tags:
            if k == SystemFields.Top:
                res.append(("Top", v))
            elif k == SystemFields.Stack:
                res.append(("Stack", str(v).lower()))
            else:
                assert isinstance(k, str)
                res.append((k, v))
        return res


class DebugSerializer(SerializerBase):
    def __init__(self, tag_postprocessor=DebugTagPostprocessor()):
        super().__init__()
        self.tag_postprocessor = tag_postprocessor

    def on_cell_content(self, content):
        if issubclass(type(content), EmptyCell):
            return "<EMPTY>"
        if issubclass(type(content), Text):
            return content.text
        if not issubclass(type(content), Sensor):
            raise Exception(f"Cannot serialize cell content of type {type(content)}")
        tags = list(content.get_tags().items())
        if self.tag_postprocessor is not None:
            tags, _ = self.tag_postprocessor.postprocess(tags)
        return "\n".join("{}={}".format(k, str(v)) for k, v in tags)
