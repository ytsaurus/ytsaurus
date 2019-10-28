import gdb
import gdb.printing


class StrokaPrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        return self.value["p"]

    def display_hint(self):
        return "string"


class YtEnumBasePrinter:
    def __init__(self, value):
        self.value = value

    def to_string(self):
        return self.value["Value"]


def ya_printer():
    pp = gdb.printing.RegexpCollectionPrettyPrinter("yandex")
    pp.add_printer("Stroka", r"^Stroka$", StrokaPrinter)
    pp.add_printer("YtEnumBase", r"^NYT::TEnumBase<", YtEnumBasePrinter)
    return pp


def ya_register(obj):
    gdb.printing.register_pretty_printer(obj, ya_printer())
