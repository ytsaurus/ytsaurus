#!/usr/bin/env python3
"""Build a flamegraph from a ytprof pprof profile, optionally split at the top by trace labels.

The Flow worker attaches profiling tags (ytflow.computation_id, ytflow.computation_class_name)
to CPU profiler samples, so a profile captured from /ytprof/cpu/profile can be sliced by
computation. This tool folds such a profile into a flamegraph whose topmost frames are those
tags, i.e. the flamegraph is split by computation. It also renders plain flamegraphs for other
profiles (e.g. /ytprof/heap) when no grouping is requested.

Self-contained: parses the pprof protobuf directly and depends only on the Python standard
library. Full C++ names (with template parameters) are recovered by demangling the mangled
symbol from the profile's Function.system_name via c++filt, because the pretty name pprof
stores is truncated. See README.md for how to capture a profile from a running service.
"""

import argparse
import gzip
import html
import shutil
import subprocess
import sys
from collections import defaultdict

UNLABELED = "(unlabeled)"


# ---------------------------------------------------------------------------
# Minimal pprof (perftools profile.proto) protobuf decoding.
# ---------------------------------------------------------------------------


def _varint(buf, pos):
    result = shift = 0
    while True:
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, pos
        shift += 7


def _fields(buf):
    """Yield (field_number, value) for a protobuf message; value is int (varint) or bytes."""
    pos, n = 0, len(buf)
    while pos < n:
        key, pos = _varint(buf, pos)
        wire = key & 7
        field = key >> 3
        if wire == 0:
            val, pos = _varint(buf, pos)
        elif wire == 2:
            length, pos = _varint(buf, pos)
            val = buf[pos : pos + length]
            pos += length
        elif wire == 1:
            val = buf[pos : pos + 8]
            pos += 8
        elif wire == 5:
            val = buf[pos : pos + 4]
            pos += 4
        else:
            raise ValueError("unsupported protobuf wire type %d" % wire)
        yield field, val


def _packed(buf):
    pos, n, out = 0, len(buf), []
    while pos < n:
        v, pos = _varint(buf, pos)
        out.append(v)
    return out


def _repeated_ints(acc, value):
    if isinstance(value, int):
        acc.append(value)
    else:
        acc.extend(_packed(value))


def parse_profile(raw):
    string_table = []
    functions = {}  # function id -> (name index, system_name index)
    locations = {}  # location id -> [function id, ...] (leaf -> root within the location)
    sample_types = []  # [(type index, unit index), ...]
    samples = []  # [(location ids, values, {label key index: str index}), ...]

    for field, val in _fields(raw):
        if field == 6:  # string_table (repeated, in index order)
            string_table.append(val.decode("utf-8", "replace"))
        elif field == 5:  # Function
            fid = name = system_name = 0
            for f, v in _fields(val):
                if f == 1:
                    fid = v
                elif f == 2:
                    name = v
                elif f == 3:
                    system_name = v
            functions[fid] = (name, system_name)
        elif field == 4:  # Location
            lid, func_ids = 0, []
            for f, v in _fields(val):
                if f == 1:
                    lid = v
                elif f == 4:  # Line
                    for lf, lv in _fields(v):
                        if lf == 1:
                            func_ids.append(lv)
            locations[lid] = func_ids
        elif field == 1:  # sample_type (ValueType)
            t = u = 0
            for f, v in _fields(val):
                if f == 1:
                    t = v
                elif f == 2:
                    u = v
            sample_types.append((t, u))
        elif field == 2:  # Sample
            loc_ids, values, labels = [], [], {}
            for f, v in _fields(val):
                if f == 1:
                    _repeated_ints(loc_ids, v)
                elif f == 2:
                    _repeated_ints(values, v)
                elif f == 3:  # Label
                    k = s = 0
                    for lf, lv in _fields(v):
                        if lf == 1:
                            k = lv
                        elif lf == 2:
                            s = lv
                    if s:
                        labels[k] = s
            samples.append((loc_ids, values, labels))

    return string_table, functions, locations, sample_types, samples


# ---------------------------------------------------------------------------
# Symbol names: recover full C++ names by demangling Function.system_name.
# ---------------------------------------------------------------------------


def _demangle(mangled_names):
    """Batch-demangle via c++filt (system, or 'ya tool c++filt'); {} if unavailable."""
    if shutil.which("c++filt"):
        cmd = ["c++filt"]
    elif shutil.which("ya"):
        cmd = ["ya", "tool", "c++filt"]
    else:
        sys.stderr.write("warning: c++filt not found; using truncated names\n")
        return {}
    ordered = list(mangled_names)
    try:
        out = subprocess.run(
            cmd,
            input="\n".join(ordered) + "\n",
            capture_output=True,
            text=True,
            check=True,
        ).stdout.splitlines()
    except (OSError, subprocess.CalledProcessError) as err:
        sys.stderr.write("warning: c++filt failed (%s); using truncated names\n" % err)
        return {}
    return {m: d for m, d in zip(ordered, out)}


def resolve_names(string_table, functions, demangle=True):
    def s(i):
        return string_table[i] if 0 <= i < len(string_table) else ""

    mangled = {s(sys_i) for _name_i, sys_i in functions.values() if s(sys_i)}
    demap = _demangle(mangled) if (demangle and mangled) else {}

    names = {}
    for fid, (name_i, sys_i) in functions.items():
        system_name = s(sys_i)
        names[fid] = demap.get(system_name) or s(name_i) or system_name or "[unknown]"
    return names


# ---------------------------------------------------------------------------
# Folding: pprof samples -> {(root frames ... leaf frames): value}.
# ---------------------------------------------------------------------------


def value_index(string_table, sample_types, wanted):
    for i, (t, _unit) in enumerate(sample_types):
        if 0 <= t < len(string_table) and string_table[t] == wanted:
            return i
    return None


def fold(profile, names, group_keys, value_idx):
    string_table, _functions, locations, _sample_types, samples = profile

    folded = defaultdict(int)
    for loc_ids, values, labels in samples:
        if value_idx >= len(values):
            continue
        weight = values[value_idx]
        if weight <= 0:
            continue

        label_map = {string_table[k]: string_table[s] for k, s in labels.items()}
        roots = []
        for key in group_keys:
            if key in label_map:
                roots.append(label_map[key])
            else:
                break
        if group_keys and not roots:
            roots = [UNLABELED]

        leaf_first = []
        for lid in loc_ids:
            for func_id in locations.get(lid, []):
                leaf_first.append(names.get(func_id, "[unknown]"))
        if not leaf_first:
            leaf_first = ["[unknown]"]

        stack = tuple(roots) + tuple(reversed(leaf_first))
        folded[stack] += weight
    return folded


# ---------------------------------------------------------------------------
# Tree + SVG flamegraph rendering (self-contained, click-to-zoom + search).
# ---------------------------------------------------------------------------


class Node:
    __slots__ = ("name", "value", "children")

    def __init__(self, name):
        self.name = name
        self.value = 0
        self.children = {}


def build_tree(folded):
    root = Node("all")
    for stack, weight in folded.items():
        root.value += weight
        node = root
        for frame in stack:
            child = node.children.get(frame)
            if child is None:
                child = Node(frame)
                node.children[frame] = child
            child.value += weight
            node = child
    return root


def _color(name):
    # Deterministic warm palette, similar spirit to the classic flamegraph "hot" colors.
    h = 0
    for ch in name:
        h = (h * 31 + ord(ch)) & 0xFFFFFFFF
    r = 205 + (h % 50)
    g = 20 + (h // 50 % 210)
    b = 20 + (h // 10500 % 55)
    return "rgb(%d,%d,%d)" % (r, g, b)


FRAME_H = 16
FONT = 12
CHAR_W = 7.2
PAD = 24


def render_svg(root, width, title, unit):
    total = root.value or 1
    scale = width / total

    rects = []  # (x, depth, w, name, value)
    max_depth = [0]

    def layout(node, x, depth):
        w = node.value * scale
        if depth > 0:  # skip the virtual "all" root frame
            rects.append((x, depth - 1, w, node.name, node.value))
            max_depth[0] = max(max_depth[0], depth - 1)
        cx = x
        for child in sorted(node.children.values(), key=lambda c: c.name):
            layout(child, cx, depth + 1)
            cx += child.value * scale

    layout(root, 0.0, 0)

    height = (max_depth[0] + 1) * FRAME_H + PAD * 2 + FRAME_H
    top = height - PAD - FRAME_H

    out = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" '
        'viewBox="0 0 %d %d" font-family="Verdana,monospace" font-size="%d">' % (width, height, width, height, FONT),
        '<rect width="%d" height="%d" fill="#f8f8f8"/>' % (width, height),
        '<text x="%d" y="%d" font-size="%d" font-weight="bold" text-anchor="middle">%s</text>'
        % (width // 2, FRAME_H, FONT + 2, html.escape(title)),
        '<text id="details" x="8" y="%d">click a frame to zoom, Search to highlight</text>' % (height - 8),
        '<text id="search" x="%d" y="%d" fill="#3030c0" style="cursor:pointer">Search</text>' % (width - 70, FRAME_H),
    ]

    for x, depth, w, name, value in rects:
        if w < 0.2:
            continue
        y = top - depth * FRAME_H
        pct = 100.0 * value / total
        tip = "%s (%s %s, %.2f%%)" % (name, format(value, ","), unit, pct)
        out.append('<g class="f" data-x="%.3f" data-w="%.3f" data-n="%s">' % (x, w, html.escape(name, quote=True)))
        out.append('<title>%s</title>' % html.escape(tip))
        out.append(
            '<rect x="%.3f" y="%d" width="%.3f" height="%d" fill="%s" rx="1" ry="1"/>'
            % (x, y, w, FRAME_H - 1, _color(name))
        )
        if w > 2 * CHAR_W:
            maxchars = int(w / CHAR_W)
            label = name if len(name) <= maxchars else name[: max(0, maxchars - 1)] + "…"
            out.append('<text x="%.3f" y="%d">%s</text>' % (x + 2, y + FRAME_H - 4, html.escape(label)))
        out.append('</g>')

    out.append(_JS % {"width": width, "char_w": CHAR_W})
    out.append('</svg>')
    return "\n".join(out)


_JS = """<script><![CDATA[
var W = %(width)d, CHAR_W = %(char_w)s;
var frames = Array.prototype.slice.call(document.getElementsByClassName("f"));
var details = document.getElementById("details");
function orig(g){return {x: parseFloat(g.getAttribute("data-x")), w: parseFloat(g.getAttribute("data-w"))};}
function apply(x0, w0){
  var scale = W / w0;
  frames.forEach(function(g){
    var o = orig(g), rect = g.getElementsByTagName("rect")[0], txt = g.getElementsByTagName("text")[0];
    var nx = (o.x - x0) * scale, nw = o.w * scale;
    if (nx + nw <= 0 || nx >= W){ g.style.display = "none"; return; }
    g.style.display = "";
    var rx = nx < 0 ? 0 : nx, rw = nx < 0 ? nw + nx : nw;
    if (rx + rw > W) rw = W - rx;
    rect.setAttribute("x", rx); rect.setAttribute("width", rw);
    if (txt){
      if (rw > 2 * CHAR_W){
        txt.style.display = "";
        txt.setAttribute("x", rx + 2);
        var name = g.getAttribute("data-n"), max = Math.floor(rw / CHAR_W);
        txt.textContent = name.length > max ? name.slice(0, Math.max(0, max - 1)) + "\\u2026" : name;
      } else { txt.style.display = "none"; }
    }
  });
}
frames.forEach(function(g){
  g.style.cursor = "pointer";
  g.addEventListener("click", function(){ var o = orig(g); apply(o.x, o.w); });
  g.addEventListener("mouseover", function(){ details.textContent = g.getElementsByTagName("title")[0].textContent; });
});
document.getElementById("search").addEventListener("click", function(){
  var term = prompt("Search (substring, empty to reset):"), matched = 0;
  frames.forEach(function(g){
    var rect = g.getElementsByTagName("rect")[0];
    if (term && g.getAttribute("data-n").indexOf(term) >= 0){
      rect.setAttribute("stroke", "#000"); rect.setAttribute("stroke-width", "1"); matched++;
    } else { rect.removeAttribute("stroke"); rect.removeAttribute("stroke-width"); }
  });
  details.textContent = term ? (matched + " frames match \\"" + term + "\\"") : "search cleared";
});
]]></script>"""


def write_folded(folded, stream):
    for stack, weight in sorted(folded.items()):
        stream.write("%s %d\n" % (";".join(stack), weight))


def main():
    parser = argparse.ArgumentParser(
        description="Build a flamegraph from a ytprof pprof profile, split at the top by " "trace-context labels.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Capture a CPU profile from a running Flow worker, then render:\n"
            "  curl -sg 'http://[<worker-ipv6>]:80/ytprof/cpu/profile?d=30s' -o cpu.pb.gz\n"
            "  flamegraph cpu.pb.gz -o flame.svg\n\n"
            "Plain heap flamegraph (no tag split):\n"
            "  curl -sg 'http://[<worker-ipv6>]:80/ytprof/heap' -o heap.pb.gz\n"
            "  flamegraph heap.pb.gz --group '' --value space -o heap.svg\n\n"
            "Worker monitoring addresses come from the flow view:\n"
            "  yt --proxy <cluster> flow get-flow-view <pipeline_path>  # see worker_statuses\n"
        ),
    )
    parser.add_argument("profile", help="pprof profile (.pb.gz or .pb) from a /ytprof endpoint")
    parser.add_argument(
        "-o",
        "--output",
        default="flame.svg",
        help="output path; a '.folded' extension emits collapsed stacks instead of SVG",
    )
    parser.add_argument(
        "--group",
        default="ytflow.computation_class_name,ytflow.computation_id",
        help="comma-separated label keys used as the top frames (empty for a plain flamegraph)",
    )
    parser.add_argument("--value", default="cpu", help="pprof sample value type to weigh by (e.g. cpu, space)")
    parser.add_argument(
        "--no-demangle", action="store_true", help="keep pprof's truncated names instead of demangling full C++ names"
    )
    parser.add_argument("--title", default=None, help="flamegraph title")
    parser.add_argument("--width", type=int, default=1600, help="image width in pixels")
    args = parser.parse_args()

    with open(args.profile, "rb") as f:
        blob = f.read()
    if blob[:2] == b"\x1f\x8b":
        blob = gzip.decompress(blob)

    profile = parse_profile(blob)
    string_table, functions, _locations, sample_types, _samples = profile

    value_idx = value_index(string_table, sample_types, args.value)
    if value_idx is None:
        available = ", ".join(string_table[t] for t, _ in sample_types) or "(none)"
        value_idx = len(sample_types) - 1
        sys.stderr.write(
            "warning: value type %r not found (available: %s); using %r\n"
            % (args.value, available, string_table[sample_types[value_idx][0]])
        )
    unit_name = string_table[sample_types[value_idx][1]] if sample_types else "samples"

    names = resolve_names(string_table, functions, demangle=not args.no_demangle)
    group_keys = [k for k in args.group.split(",") if k]
    folded = fold(profile, names, group_keys, value_idx)
    if not folded:
        sys.exit("no samples with value type %r found in %s" % (args.value, args.profile))

    if args.output.endswith(".folded"):
        with open(args.output, "w") as f:
            write_folded(folded, f)
    else:
        default_title = (
            ("flamegraph split by %s" % ", ".join(group_keys)) if group_keys else "flamegraph (%s)" % unit_name
        )
        svg = render_svg(build_tree(folded), args.width, args.title or default_title, unit_name)
        with open(args.output, "w") as f:
            f.write(svg)

    sys.stderr.write(
        "wrote %s (%d folded stacks, total %s %s)\n"
        % (args.output, len(folded), format(sum(folded.values()), ","), unit_name)
    )


if __name__ == "__main__":
    main()
