import yaml
import marisa_trie
import six
from collections import defaultdict


def load_yaml_config(path):
    try:
        with open(path) as migrations_file:
            return yaml.safe_load(migrations_file)
    except OSError as e:
        raise Exception("Configuration file {!r} reading error: {}.", path, e.strerror)
    except yaml.YAMLError as e:
        raise Exception("Configuration file {!r} parsing error: {}.", path, e)


class MigrationsConfig:
    def __init__(self, config=None, section="flake8"):
        # type: (dict, str) -> None
        if config is None:
            self.prefix_trie = marisa_trie.BytesTrie([])
            return

        prefix_to_ignores = defaultdict(set)

        if section in config:
            flake8_ignore_config = config[section]
        else:
            flake8_ignore_config = config['migrations']

        for value in flake8_ignore_config.values():
            ignores = set(value.get('ignore', []))  # set
            prefixes = value.get('prefixes', [])  # list
            for prefix in prefixes:
                prefix_to_ignores[prefix] |= ignores
        items = []
        for key, value in prefix_to_ignores.items():
            unicoded_key = six.ensure_text(key)
            for subvalue in value:
                items.append(
                    (unicoded_key, six.ensure_binary(subvalue))
                )  # key should be unicode. value should be a binary string
        self.prefix_trie = marisa_trie.BytesTrie(items)

    def get_exceptions(self, prefix):
        # type: (str) -> set[str]
        prefixes = self.prefix_trie.prefixes(six.ensure_text(prefix))
        answer = set()
        for prefix in prefixes:
            answer.update(self.prefix_trie.get(prefix))
        return {six.ensure_str(a) for a in answer}

    def is_skipped(self, prefix):
        # type: (str) -> bool
        exceptions = self.get_exceptions(prefix)
        return "*" in exceptions
