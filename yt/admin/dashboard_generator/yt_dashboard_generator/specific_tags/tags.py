class SpecificTag():
    def __init__(self, s):
        self.value = s

    def __str__(self):
        return self.value

    def __repr__(self):
        return f"{self.name}[{self.value}]"

    def __hash__(self):
        return hash((self.name, self.value))

    def __eq__(self, other):
        return type(self) == type(other) and self.name == other.name and self.value == other.value

    @classmethod
    def make_new(cls, name):
        Cls = type(name, (cls,), {})
        Cls.name = name
        return Cls


class BackendTag(SpecificTag):
    pass


TemplateTag = SpecificTag.make_new("TemplateTag")


class Regex():
    def __init__(self, value):
        self.value = value
