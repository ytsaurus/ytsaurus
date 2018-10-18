from yt.wrapper import YtClient
from plotly.offline import init_notebook_mode, iplot
import plotly.graph_objs as go
import json

def get_units_info(unit_type, cluster_name):
    client = YtClient(proxy="hahn")
    client.config['pickling']['module_filter'] = lambda module: 'hashlib' not in getattr(module, '__name__', '')
    
    units_info = {}
    for row in client.read_table("//home/dev/ivanashevi/resource_consumption/{}_info".format(unit_type)):
        if row["cluster_name"] == cluster_name:
            units_info[row["name"]] = {
                "statistics": row["statistics"],
                "children": row.get("children", []),
            }
    return units_info


def sort_children(unit_name, units_info, data_type):
    return sorted(
        units_info[unit_name]["children"],
        key=lambda x: units_info[x]["statistics"][data_type],
        reverse=True,
    )


def draw_hist(unit_type, root, data_type, cluster_name="all"):
    """
    Draw resource consumption histogram using data,
    collected by collect_data function into service_info and department_info tables
    Usage example: draw_hist("department", "Yandex", "cpu_consumption", "hahn") to draw cpu_consumption
    histogram in departments tree with root "Yandex" for operations on cluster hahn
    """
    init_notebook_mode(connected=True)
    units_info = get_units_info(unit_type, cluster_name)
    traces = []
    if not units_info[root]["children"]:
        units_info[root]["children"] = [root]
    for child in sort_children(root, units_info, data_type):
        if not units_info[child]["children"]:
            units_info[child]["children"] = [child]
        for grandchild in sort_children(child, units_info, data_type):
            if units_info[grandchild]["statistics"][data_type]:
                traces.append(go.Bar(
                    x=[child],
                    y=[units_info[grandchild]["statistics"][data_type]],
                    name=grandchild,
                    hoverinfo="name+y",
                    hoverlabel=dict(
                        namelength=-1,
                    ),
                ))
    iplot(dict(
        data = traces,
        layout = go.Layout(
            xaxis = dict(title = root),
            barmode="stack",
            hovermode = "closest",
            showlegend=False
        ),
    ))


def make_report(unit_type, root, data_type, max_depth=3, cluster_name="all"):
    """
    Make resource consumption json report using data,
    collected by collect_data function into service_info and department_info tables
    Result is saved into file report.json
    Usage example: make_report("department", "Yandex", "cpu_consumption", 6, "hahn") to make cpu_consumption
    report in departments tree with root "Yandex" for 6th level of nesting for operations on cluster hahn
    """
    units_info = get_units_info(unit_type, cluster_name)
    total_amount = units_info[root]["statistics"][data_type]
    
    def get_unit_info(unit_name, result, depth):
        value = units_info[unit_name]["statistics"][data_type]
        percent = 100.0 * value / total_amount
        unit_description = "{0} - ({1:0.5f}% / {2:.5E})".format(unit_name, percent, value)
        result[unit_description] = None
        if depth + 1 < max_depth and units_info[unit_name]["children"]:
            result[unit_description] = {}
            for child in sort_children(unit_name, units_info, data_type):
                get_unit_info(child, result[unit_description], depth + 1)
    
    result = {}
    get_unit_info(root, result, 0)
    with open("report.json", "w") as text_file:
        text_file.write(json.dumps(result, indent=4))
