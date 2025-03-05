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
        return type(self) is type(other) and self.name == other.name and \
            self.value == other.value

    @classmethod
    def make_new(cls, name):
        Cls = type(name, (cls,), {})
        Cls.name = name
        return Cls


class BackendTag(SpecificTag):
    pass


class DuplicateTag():
    """
    Used to denote that a certain tag should be deliberately present
    in the query more than once.
    """
    def __init__(self, tag, tie_breaker=1):
        self.tag = tag
        self.tie_breaker = tie_breaker

    def __str__(self):
        return str(self.tag)

    def __repr__(self):
        return f"{self.tag}#{self.tie_breaker}"

    def __hash__(self):
        return hash((self.tag, self.tie_breaker))

    def __eq__(self, other):
        return type(self) is type(other) and self.tag == other.tag and \
            self.tie_breaker == other.tie_breaker


TemplateTag = SpecificTag.make_new("TemplateTag")


class Regex():
    def __init__(self, value):
        self.value = value
