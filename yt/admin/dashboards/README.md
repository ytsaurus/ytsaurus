Dashboard generator
-------------------

All-in-one DSL for Monitoring and Grafana dashboards

## Bootstrap

Most likely you'll only need to issue a token or an API key. Usual URLs and endpoints are provided by the library by default.

### Monitoring

*   Put Solomon OAuth token into ~/.solomon/token or provide it via `--solomon-token` (sic!).
*   Use `--monitoring-endpoint` for specifying custom endpoint.

### Grafana

*   Issue an api key via [https://grafana.ytpublicdemo.tech/org/apikeys.](https://grafana.ytpublicdemo.tech/org/apikeys.) Put a path to it into `GRAFANA_API_KEY_PATH` variable or provide the api key via `--grafana-api-key`.
*   Use `--grafana-base-url` for specifying endpoint of your grafana.

## Top-level dashboard list

Before using this tool you must create a dashboard in the corresponding system. Specify its name and description as you wish, the tool will not modify them. Then add your dashboard to the `dashboards` dict in `__main__.py` in the following format:

```text-plain
"short-name": {
  "func": callback,
  <backend>: {
      "id": <dashboard_id>,
      "args": [args], # optional
      "postprocessor": postprocessor # optional
  },
  <backend>: <dashboard_id>, # short form in case if there are no additional arguments
}
```

`callback` is a function that returns a generated dashboard. `args` will be provided to callback in expanded form (that is, `callback(*args)`). Postprocessors are not documented yet, sorry. Dashboard id is taken from the URL, it looks like `smth-human-readable` in Solomon, `mong0ik4jjrpnp1mrdtq` in Monitoring and `t141_FDVz` in Grafana.

## Supported commands

Except for `list`, all commands take a repeated positional argument representing dashboard short name and a repeated `--backend` argument.

*   `show`: print current upstream version of the dashboard.
*   `preview`: print the code-generated version of the dashboard.
*   `diff`: show diff between upstream and code-generated versions.
*   `submit`: send generated dashboard to the system.

Only `submit` command is guaranteed to be supported by all backends.

## DSL

### Taggable

Many entities implement `Taggable` interface. It has a method `value(k, v)` that typically adds a label `k=v` to all underlying charts and a bit of syntactic sugar:

*   `aggr(k)`: add a label `k=-` representing aggregation in YT profiling;
*   `all(k)`: add a label `k=*`;
*   `top(n=10, aggregation="max")`: filter top-10 entries with maximum value of maximum;
*   `stack(value=True)`: enable/disable stacking;
*   `range(min, max)`: specify min/max values for Y-axes;
*   `min(x)`, `.max(x)`: shorthand for `range` in case if only min/max should be specified.

One can provide labels that will be understood only by a certain backend and ignored by all others, e.g. `value(MonitoringTag("name"), “value”)`.

### Sensor

Sensor typically represents a simple chart. One can construct a sensor from a name string and add tags:

`Sensor("yt.tablet_node.write.data_weight.rate").aggr("host").all("user").value("tablet_cell_bundle", “bigb”)`

If  `{}` placeholders are present then the sensor becomes callable:

```text-plain
s = Sensor("yt.tablet_node.{}.{}.rate")
s("write", "data_weight").aggr("host").all("user").value("tablet_cell_bundle", "bigb")
```

### Cell

A cell in the dashboard grid. Contains a title and a sensor. Usually cells are not created directly but added to rows (see below).

In future releases it will be possible to create cells containing multiple sensors (Monitoring and Grafana only). Currently only simple charts are supported.

### Row

One or more dashboard cells. It is possible to add labels to the row before it gets any cells. This label serves as a default value for all cells in the row but will be overridden if specified in any cell explicitly. Rows are not created directly as well, they come from rowsets.

### Rowset

Rowset is a collection of rows. They are created directly, see the example below. The most interesting feature of a rowset is that it is possible to add labels to all its cells. One can add labels either at the beginning of the rowset or at the end. If some label is explicitly specified by the sensor in the cell, it will be overridden only by labels of the rowset given at the end. See the example.

```text-plain
r = (Rowset()
    .value("foo", "default_foo")
    .value("bar", "default_bar")
    .row()
        .cell("First cell", Sensor("yt.some.sensor").value("foo", "explicit_foo"))
        .cell("Second cell", Sensor("yt.some.other.sensor").value("bar", "explicit_bar"))
).owner
r2 = r.value("foo", "overridden_foo")
```

In this case `r` will contain cells with values `(explicit_foo, default_bar)` and `(default_foo, explicit_bar)` and `r2` will contain cells with values `(overridden_foo, default_bar)` and `(overridden_foo, explicit_bar)`.

Note the weird `owner` part at the end of rowset declaration. It is necessary since `.row()` and `.cell()` methods return temporary proxy objects and this is a best workaround I could've thought of.

### Dashboard

All dashboards created by the generator are grid-based. Internally the dashboard consists of rowsets. However, they have no effect on the presentation (i.e. these are not Grafana rowsets having separate titles) and are used only for setting labels to groups of cells. Here is how the dashboard can be created:

```text-plain
d = Dashboard()
d.add(build_some_rowset()) # build rowset as in the previous example
d.add(build_some_other_rowset())
d.value("cluster", "{{cluster}}") # add placeholder for Solomon dashboard
```

Rowsets may be reused between dashboards and even be added to different dashboards with different tags. For instance, it may make sense to have two similar dashboards with distinct aggregations: top-by-user aggr-by-host and aggr-by-user top-by-host.

### YT helpers

The library provides some handy snippets for YT-related dashboards. They are defined in `common/sensors.py`.

*   `yt_host`: label for setting all/aggr to the host and container simultaneously. Usage: `taggable.all(yt_host)` or `taggable.aggr(yt_host)`. Recall that all of cells, rows, rowsets and dashboards may serve as `taggable`. NB: not currently supported in Grafana.
*   `ProjectSensor`: subclass of `Sensor` having builtin `service=` labels for Monitoring and `job=` for Grafana.
*   `RpcBase`: subclass of `Sensor` providing `service_method` method adding labels `yt_service=` and `method=`. Used for `xxx_rpc` and `xxx_rpc_client` Solomon shards.
*   Shorthands for specific services defined with `ProjectSensor`: `SchedulerMemory`, `NodeTablet`, `CAInternal` and similar. Not all of them are present, feel free to add your own.
*   `SplitExeTabNodeTagPostprocessor` (defined in `yt_dashboard_generator/specific_tags/postprocessors.py`): replaces `node_xxx` service with `node_xxx|exe_node_xxx`. Useful for dashboards for cluster with split dat/exe-tab nodes. Add `"postprocessor": SplitExeTabNodeTagPostprocessor` to the Solomon/Monitoring sections of your dashboard definition.

### System-specific tags
All supported backends (Monitoring and Grafana) define a wrapper for tags that should be recognized only by a specific system. For example, here the label `"foo"` will be added only to Monitoring dashboards:

```
from yt_dashboard_generator.backends.monitoring import MonitoringTag

s = Sensor("yt.tablet_node.blabla").value(MonitoringTag("foo"), "bar")

```

### Advanced Monitoring charts
If your dashboard is meant to work only with Monitoring, you may use advanced classes and modifiers defined in `yt_dashboard_generator.monitoring`.

#### Plain expressions
Defined in `yt_dashboard_generator.backends.monitoring.sensors.PlainMonitoringExpr`. Used to define constant expressions. May contain placeholders in the form `{var}`. They will be filled by all tags provided by `.value()` methods. One may also use specific `PlainExprVariable` tag to define variables that will fill placeholders in `PlainMonitoringExpr` but will not be added to regular sensors.

```
...
r = (Rowset()
    .row()
        .cell("Some constant", PlainMonitoringExpr("constant_line({v1})"))
        .cell("Other constant", PlainMonitoringExpr("constant_line({v2})"))
        .cell("Some sensor", Sensor("yt.tablet_node.blabla"))
).owner

r = (r
    .value("v1", 123)
    .value(PlainExprVariable("v2"), 456)
    .value("foo", "bar")
)
```

In this case both placeholders will be filled by corresponding values. Tag `"foo"` will be not added to constant lines since there is no such placeholder. Both `"v1"` and `"foo"` tags will be added to the third sensor.

#### Multiple sensors in one chart
Use `yt_dashboard_generator.sensor.MultiSensors` to define multiple sensors in one chart. This is supposed to work with Grafana backend as well but is not yet supported.

```
.cell("Some name", MultiSensor(Sensor("yt.tablet_node.blabla"), PlainMonitoringExpr(constant_line(100))))
```

#### Advanced expression builder
Any `Sensor` or `PlainMonitoringExpr` may be wrapped into `yt_dashboard_generator.backends.monitoring.sensors.MonitoringExpr`. Besides being `Taggable` (and thus supporting all usual `.value(k, v)`, `.stack()`  and so), they provide additional methods and operators.

- Arithmetic operators: it is possible to add/subtract sensors and to multiply/divide a sensor by a constant.
- `alias(param)`: alias. `param` may contain dashboard parameters, e.g. `{{cluster}}`.
- `moving_avg(param)`.
- `drop_below(param)`.
- `group_by_label(label, expr)`. Only a single label is supported.
- `series_avg(label)`.
- `downsampling_aggregation(value)`. Expected an item from `DownsamplingAggregation`.

Example:

```
x = MonitoringExpr(Sensor("yt.tablet_node.blabla"))
y = MonitoringExpr(Sensor("yt.tablet_node.foobar"))
s = ((x.moving_avg("10m") / y.moving_avg("10m")) * 10).alias("funky name")
```


## Build and run
This library is supposed to be built with `ya make` and run as a self-contained binary. The executable target can bin found in `$SOURCE_ROOT/yt/admin/dashboards/yt_dashboards/bin`

Moreover this library can be used within system python3.
  1. Install venv for python playground: [how to install it](https://docs.python.org/3/library/venv.html).
  2. Install `dashboard_generator` and `dashboards` modules (in developer mode).
```
pip3 install -e $SOURCE_ROOT/yt/admin/dashboard_generator
pip3 install -e $SOURCE_ROOT/yt/admin/dashboards
```
  3. Run `main.py` within python3.
```
python3 $SOURCE_ROOT/yt/admin/dashboards/yt_dashboards/bin/main.py
```

