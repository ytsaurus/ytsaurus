import abc
from typing import Dict, Any, Optional, Iterable
import copy

from .specific_tags.tags import TemplateTag, BackendTag


class TagPostprocessorBase(abc.ABC):
    @abc.abstractmethod
    def postprocess(self, tags: Dict[str, Any], sensor_name: Optional[str]) \
            -> (Dict[str, Any], Optional[str]):
        raise NotImplementedError


class SimpleTagPostprocessor(TagPostprocessorBase):
    @abc.abstractmethod
    def process_tag(self, k, v):
        raise NotImplementedError

    def postprocess(self, tags, sensor_name):
        result = []
        for k, v in tags:
            new_tags = self.process_tag(k, v)
            if new_tags is None:
                continue

            assert isinstance(new_tags, Iterable)

            # Is new_tags a single (k, v) pair or a list of those?
            if isinstance(new_tags[0], Iterable) and not isinstance(new_tags[0], str):
                result.extend(new_tags)
            else:
                result.append(new_tags)
        return result, sensor_name


class MultiTagPostprocessor(TagPostprocessorBase):
    def __init__(self, *postprocessors):
        super().__init__()
        self.simple_postprocessors = []
        self.final_postprocessors = []
        for p in postprocessors:
            if isinstance(p, SimpleTagPostprocessor):
                self.simple_postprocessors.append(p)
            else:
                self.final_postprocessors.append(p)

    def postprocess(self, tags, sensor_name):
        # First iterate through all simple ones until fixed point is reached.
        while True:
            prev_tags = copy.copy(tags)
            for p in self.simple_postprocessors:
                tags, _ = p.postprocess(prev_tags, sensor_name)
            if prev_tags == tags:
                break

        # Then run all nontrivial postprocessors in order.
        for p in self.final_postprocessors:
            tags, sensor_name = p.postprocess(tags, sensor_name)

        return tags, sensor_name


class TemplateTagPostprocessor(SimpleTagPostprocessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.template_keys = []

    @abc.abstractmethod
    def expand_template(self, k, v) -> (str, str):
        raise NotImplementedError

    def process_tag(self, k, v):
        if isinstance(v, TemplateTag):
            self.template_keys.append(k)
            return self.expand_template(k, v.value)
        else:
            return k, v


class BackendTagPostprocessor(SimpleTagPostprocessor):
    def __init__(self, backend_cls):
        super().__init__()
        self.backend_cls = backend_cls

    def process_tag(self, k, v):
        if isinstance(k, BackendTag):
            if isinstance(k, self.backend_cls):
                return (str(k), v)
            return None
        else:
            return k, v


class DollarTemplateTagPostprocessor(TemplateTagPostprocessor):
    def expand_template(self, k, v):
        return (k, "$" + v)


class MustacheTemplateTagPostprocessor(TemplateTagPostprocessor):
    def expand_template(self, k, v):
        return (k, "{{" + v + "}}")
