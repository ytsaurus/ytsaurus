def break_long_lines(s, max_width=50):
    num_leading_spaces = len(s) - len(s.lstrip(" "))
    s = s.lstrip(" ")
    separator = " " * (num_leading_spaces + 4)
    return " " * num_leading_spaces + f" \\\n{separator}".join(s[i:i+max_width] for i in range(0, len(s), max_width))


def break_long_lines_in_multiline_cell(cell, max_width=50):
    return "\n".join(break_long_lines(s, max_width) for s in cell.split("\n"))


def pretty_dump_fixed_indent(x, offset=""):
    INDENT = "  "
    if issubclass(type(x), list):
        if not x:
            return "[]"
        res = "[\n"
        for y in x:
            res += offset + INDENT + pretty_dump_fixed_indent(y, offset + INDENT) + ",\n"
        res += offset + "]"
        return res
    elif issubclass(type(x), dict):
        if not x:
            return "{}"
        res = "{\n"
        for k, v in x.items():
            pv = pretty_dump_fixed_indent(v, offset + INDENT)
            res += offset + INDENT + f"{k}: {pv},\n"
        res += offset + "}"
        return res
    else:
        return str(x)


def pretty_print_fixed_indent(x):
    print(pretty_dump_fixed_indent(x))


def monitoring_to_grafana_unit(monitoring_unit):
    grafana_units = {
        "UNIT_NONE": "short",
        "UNIT_COUNT": "count",
        "UNIT_PERCENT": "percent",
        "UNIT_PERCENT_UNIT": "percentunit",
        "UNIT_NANOSECONDS": "ns",
        "UNIT_MICROSECONDS": "µs",
        "UNIT_MILLISECONDS": "ms",
        "UNIT_SECONDS": "s",
        "UNIT_MINUTES": "m",
        "UNIT_HOURS": "h",
        "UNIT_DAYS": "d",
        "UNIT_BITS_SI": "bits",
        "UNIT_BYTES_SI": "bytes",
        "UNIT_KILOBYTES": "kbytes",
        "UNIT_MEGABYTES": "mbytes",
        "UNIT_GIGABYTES": "gbytes",
        "UNIT_TERABYTES": "tbytes",
        "UNIT_PETABYTES": "pbytes",
        "UNIT_EXABYTES": "ebytes",
        "UNIT_BITS_IEC": "bits(iec)",
        "UNIT_BYTES_IEC": "bytes(iec)",
        "UNIT_KIBIBYTES": "kibytes",
        "UNIT_MEBIBYTES": "mibytes",
        "UNIT_GIBIBYTES": "gibytes",
        "UNIT_TEBIBYTES": "tibytes",
        "UNIT_PEBIBYTES": "pibytes",
        "UNIT_EXBIBYTES": "eibytes",
        "UNIT_REQUESTS_PER_SECOND": "reqps",
        "UNIT_OPERATIONS_PER_SECOND": "ops",
        "UNIT_WRITES_PER_SECOND": "wps",
        "UNIT_READS_PER_SECOND": "rps",
        "UNIT_PACKETS_PER_SECOND": "pps",
        "UNIT_IO_OPERATIONS_PER_SECOND": "iops",
        "UNIT_COUNTS_PER_SECOND": "cps",
        "UNIT_BITS_SI_PER_SECOND": "bitsps",
        "UNIT_BYTES_SI_PER_SECOND": "Bps",
        "UNIT_KILOBITS_PER_SECOND": "Kbps",
        "UNIT_KILOBYTES_PER_SECOND": "KBps",
        "UNIT_MEGABITS_PER_SECOND": "Mbps",
        "UNIT_MEGABYTES_PER_SECOND": "MBps",
        "UNIT_GIGABITS_PER_SECOND": "Gbps",
        "UNIT_GIGABYTES_PER_SECOND": "GBps",
        "UNIT_TERABITS_PER_SECOND": "Tbps",
        "UNIT_TERABYTES_PER_SECOND": "TBps",
        "UNIT_PETABITS_PER_SECOND": "Pbps",
        "UNIT_PETABYTES_PER_SECOND": "PBps",
        "UNIT_BITS_IEC_PER_SECOND": "bitsps(iec)",
        "UNIT_BYTES_IEC_PER_SECOND": "Bps(iec)",
        "UNIT_KIBIBITS_PER_SECOND": "Kibps",
        "UNIT_KIBIBYTES_PER_SECOND": "KiBps",
        "UNIT_MEBIBITS_PER_SECOND": "Mibps",
        "UNIT_MEBIBYTES_PER_SECOND": "MiBps",
        "UNIT_GIBIBITS_PER_SECOND": "Gibps",
        "UNIT_GIBIBYTES_PER_SECOND": "GiBps",
        "UNIT_TEBIBITS_PER_SECOND": "Tibps",
        "UNIT_TEBIBYTES_PER_SECOND": "TiBps",
        "UNIT_PEBIBITS_PER_SECOND": "Pibps",
        "UNIT_PEBIBYTES_PER_SECOND": "PiBps",
        "UNIT_DATETIME_UTC": "dateTimeAsIso",
        "UNIT_DATETIME_LOCAL": "dateTimeAsLocal",
        "UNIT_HERTZ": "hertz",
        "UNIT_KILOHERTZ": "khz",
        "UNIT_MEGAHERTZ": "mhz",
        "UNIT_GIGAHERTZ": "ghz",
        "UNIT_DOLLAR": "currencyUSD",
        "UNIT_EURO": "currencyEUR",
        "UNIT_ROUBLE": "currencyRUB",
        "UNIT_CELSIUS": "celsius",
        "UNIT_FAHRENHEIT": "fahrenheit",
        "UNIT_KELVIN": "kelvin",
        "UNIT_FLOP_PER_SECOND": "FLOP/s",
        "UNIT_KILOFLOP_PER_SECOND": "kFLOP/s",
        "UNIT_MEGAFLOP_PER_SECOND": "MFLOP/s",
        "UNIT_GIGAFLOP_PER_SECOND": "GFLOP/s",
        "UNIT_PETAFLOP_PER_SECOND": "PFLOP/s",
        "UNIT_EXAFLOP_PER_SECOND": "EFLOP/s",
        "UNIT_METERS_PER_SECOND": "m/s",
        "UNIT_KILOMETERS_PER_HOUR": "km/h",
        "UNIT_MILES_PER_HOUR": "mph",
        "UNIT_MILLIMETER": "lengthmm",
        "UNIT_CENTIMETER": "lengthcm",
        "UNIT_METER": "lengthm",
        "UNIT_KILOMETER": "lengthkm",
        "UNIT_MILE": "lengthmi",
        "UNIT_PPM": "ppm",
        "UNIT_EVENTS_PER_SECOND": "eps",
        "UNIT_PACKETS": "packets",
        "UNIT_DBM": "dBm",
        "UNIT_VIRTUAL_CPU": "vcpu",
        "UNIT_MESSAGES_PER_SECOND": "mps"
    }
    return grafana_units.get(monitoring_unit, "short")
