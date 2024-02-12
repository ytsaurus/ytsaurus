from .taggable import Taggable, SystemFields
from .serializer import DebugSerializer

from tabulate import tabulate


class Cell(Taggable):
    """ A dashboard cell: a title and a sensor.

    Calling |value| calls |value| of the underlying sensor.
    """
    def __init__(self, title, sensor, yaxis_label=None, display_legend=None):
        self.title = title
        self.sensor = sensor
        self.yaxis_to_label = {}
        if isinstance(yaxis_label, str):
            self.yaxis_to_label[SystemFields.LeftAxis] = yaxis_label
        elif isinstance(yaxis_label, dict):
            self.yaxis_to_label = yaxis_label
        else:
            pass
        self.display_legend = display_legend

    def value(self, key, value):
        if self.sensor is not None:
            self.sensor = self.sensor.value(key, value)
        return self

    def serialize(self, begin_values, end_values, serializer):
        s = self.sensor
        for k, v in begin_values[::-1]:
            s = s.value(k, v, overwrite=False)
        for k, v in end_values:
            s = s.value(k, v)
        s = serializer.on_cell_content(s)
        return serializer.on_cell(self, s)

    def set_yaxis_label(self, label, axis=SystemFields.LeftAxis):
        assert axis in (SystemFields.LeftAxis, SystemFields.RightAxis)
        self.yaxis_to_label[axis] = label

    def set_display_legend(self, value):
        self.display_legend = value


class Row(Taggable):
    """ A dashboard row, that is, a number of cells.

    Calling |value| calls |value| of all underlying cells added at the moment.
    """
    def __init__(self, owner, height=None):
        self.owner = owner
        self.cells = []
        self.values = []
        self.height = height

    def value(self, key, value):
        assert not self.cells, "Cannot add tags to a nonempty row"
        self.values.append((key, value))
        return self

    def serialize(self, begin_values, end_values, serializer):
        begin_values = begin_values + self.values
        cells = [cell.serialize(begin_values, end_values, serializer) for cell in self.cells]
        return serializer.on_row(self, cells)

    def cell(self, title, sensor, yaxis_label=None, display_legend=None):
        self.cells.append(Cell(title, sensor, yaxis_label=yaxis_label, display_legend=display_legend))
        return self

    def row(self, height=None):
        return self.owner.row(height)


class Rowset(Taggable):
    """ A number of dashboard rows, usually having some tags in common.

    Calling |value| is allowed only before or after adding all rows. In the
    first case, |value| will be called for all underlying sensors without
    overwriting; in the second, overwrite may happen.

    Usage example follows.

    r1 = (Rowset()
        .all("host")
        .stack(False)
        .row()
            .cell("Some title", MyFirstSensor())
            .cell("Other title", MySecondSensor().stack(True).aggr("host"))
        .row()
            .cell(...)
            .cell(...)
    )

    # Override |host| for both sensors though it was explicitly set in 2nd sensor.
    r2 = r.value("host", "man1-1234")

    Now r1's first row will be
        my_first_sensor: host=*, stack=False
        my_second_sensor: host=Aggr, stack=True
    And r2's:
        my_first_sensor: host=man1-1234, stack=False
        my_second_sensor: host=man1-1234, stack=True
    """

    def __init__(self):
        self.begin_values = []
        self.end_values = []
        self.rows = []

    def value(self, key, value):
        if self.rows:
            self.end_values.append((key, value))
        else:
            self.begin_values.append((key, value))
        return self

    def row(self, height=None):
        assert not self.end_values, \
            "Tags can be added to the rowset only at the beginning or at the end"
        self.rows.append(Row(self, height=height))
        return self.rows[-1]

    def serialize(self, serializer):
        rows = [row.serialize(self.begin_values, self.end_values, serializer) for row in self.rows]
        return serializer.on_rowset(self, rows)


class Dashboard(Taggable):
    """ Dashboard as a whole.

    Usage example:

    r1 = (Rowset()
        .row()
            .cell(...)
            .cell(...)
    )

    d = Dashboard()
    d.add(r1.value("in_memory_mode", "none"))
    d.add(r1.value("in_memory_mode", "compressed"))
    """

    def __init__(self):
        self.rowsets = []
        self.has_set_values = False
        self.title = None
        self.description = None
        self.parameters = None
        self.serializer_options = {}

    def value(self, key, value):
        self.has_set_values = True
        for r in self.rowsets:
            r.value(key, value)
        return self

    def add(self, rowset):
        assert not self.has_set_values, \
            "Tags can be added to the dashboard only at the end"
        while type(rowset) is not Rowset:
            rowset = rowset.owner
        self.rowsets.append(rowset)

    def rowset(self):
        assert not self.has_set_values, \
            "Tags can be added to the dashboard only at the end"
        self.rowsets.append(Rowset())
        return self.rowsets[-1]

    def serialize(self, serializer):
        serializer.on_options(self.serializer_options)
        rowsets = []
        for rowset in self.rowsets:
            rowsets.append(rowset.serialize(serializer))
        return serializer.on_dashboard(self, rowsets)

    def show(self, tag_postprocessor=None):
        kwargs = {}
        if tag_postprocessor is not None:
            kwargs["tag_postprocessor"] = tag_postprocessor
        data = self.serialize(DebugSerializer(**kwargs))
        print(tabulate(data, tablefmt="grid"))

    def set_title(self, title):
        self.title = title

    def set_description(self, description):
        self.description = description

    def add_parameter(self, name, title, *args, backends=[]):
        if self.parameters is None:
            self.parameters = []
        self.parameters.append({"name": name, "title": title, "args": args, "backends": backends})

    def set_grafana_serializer_options(self, options):
        self.serializer_options["grafana"] = options

    def set_monitoring_serializer_options(self, options):
        self.serializer_options["monitoring"] = options
