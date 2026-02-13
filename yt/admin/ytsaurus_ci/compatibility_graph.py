from functools import cmp_to_key
from itertools import product
from typing import Any, Dict, List

from yt.admin.ytsaurus_ci import component_registry


def _satisfies_constraint(version, constraint):
    if constraint is None:
        return True

    if isinstance(constraint, list):
        return version in [str(v) for v in constraint]

    if not isinstance(constraint, str):
        return version == str(constraint)

    if "&&" in constraint:
        parts = [p.strip() for p in constraint.split("&&")]
        has_alpha = any(part.isalpha() for part in parts)

        if has_alpha:
            return any(_satisfies_constraint(version, part) for part in parts)

        return all(_satisfies_constraint(version, part) for part in parts)

    if constraint.startswith(">="):
        min_version = constraint[2:].strip()
        if version.isalpha() or min_version.isalpha():
            return False
        return _compare_versions(version, min_version) >= 0

    if constraint.startswith("<="):
        max_version = constraint[2:].strip()
        if version.isalpha() or max_version.isalpha():
            return False
        return _compare_versions(version, max_version) <= 0

    if constraint.startswith(">"):
        min_version = constraint[1:].strip()
        if version.isalpha() or min_version.isalpha():
            return False
        return _compare_versions(version, min_version) > 0

    if constraint.startswith("<"):
        max_version = constraint[1:].strip()
        if version.isalpha() or max_version.isalpha():
            return False
        return _compare_versions(version, max_version) < 0

    if version.isalpha() or constraint.isalpha():
        return version == constraint

    return version == constraint


def _compare_versions(lhs, rhs):
    if lhs.isalpha() and rhs.isalpha():
        return 0
    elif lhs.isalpha():
        return 1
    elif rhs.isalpha():
        return -1

    parts1 = [int(x) for x in lhs.split('.')]
    parts2 = [int(x) for x in rhs.split('.')]
    max_len = max(len(parts1), len(parts2))
    parts1.extend([0] * (max_len - len(parts1)))
    parts2.extend([0] * (max_len - len(parts2)))
    for p1, p2 in zip(parts1, parts2):
        if p1 < p2:
            return -1

        if p1 > p2:
            return 1

    return 0


def _extract_versions_from_constraint(constraint):
    if constraint is None:
        return []
    if isinstance(constraint, list):
        return [str(v) for v in constraint]
    if not isinstance(constraint, str):
        return [str(constraint)]
    if "&&" in constraint:
        versions = []
        parts = [p.strip() for p in constraint.split("&&")]
        for part in parts:
            versions.extend(_extract_versions_from_constraint(part))
        return versions
    if constraint.startswith(">=") or constraint.startswith("<="):
        return [constraint[2:].strip()]
    if constraint.startswith(">") or constraint.startswith("<"):
        return [constraint[1:].strip()]
    return [constraint]


class CompatibilityGraph:
    def __init__(self, registry: component_registry.VersionComponentRegistry):
        self._registry = registry
        self._component_names = set()

        visited_vertices = set()
        all_vertices = set()
        detected_components = set()
        for component_name in registry.get_components():
            self._component_names.add(component_name)
            for version in registry.get_component_versions(component_name):
                visited_vertices.add(f"{component_name}:{version}")
                constraints = registry.get_constraints(component_name, version)
                if not constraints:
                    continue

                for name, rules in constraints.items():
                    detected_components.add(name)
                    versions = _extract_versions_from_constraint(rules)
                    for version in versions:
                        all_vertices.add(f"{name}:{version}")

        diff = detected_components - self._component_names
        if len(diff) != 0:
            raise ValueError("Unregister components: ", diff)

        extra_versions = all_vertices - visited_vertices
        if len(extra_versions) > 0:
            raise ValueError(
                "Specification has components with unexplained versions",
                extra_versions,
            )

    def _is_subgraph(self, path: Dict[str, str]):
        for component, version in path.items():
            requirements = self._registry.get_constraints(component, version)
            if requirements is None:
                continue

            for sub_component, sub_constraint in requirements.items():
                sub_version = path[sub_component]
                if not _satisfies_constraint(sub_version, sub_constraint):
                    return False

                dependencies = self._registry.get_constraints(sub_component, sub_version)
                if dependencies and component in dependencies:
                    reverse_constraint = dependencies[component]
                    if not _satisfies_constraint(version, reverse_constraint):
                        return False

        return True

    def find_all_test_suites(self, constraints: Dict[str, Any] = None) -> List[Dict[str, str]]:
        if not self._component_names:
            return []

        if constraints is None:
            constraints = {}
        component_versions = {}
        for comp in self._component_names:
            versions = self._registry.get_component_versions(comp)
            if comp in constraints:
                versions = [v for v in versions if _satisfies_constraint(v, constraints[comp])]
            component_versions[comp] = versions

        all_combinations = product(
            *sorted([sorted([(comp, ver) for ver in versions]) for comp, versions in component_versions.items()])
        )

        compatible_paths = []
        for combination in all_combinations:
            path = dict(combination)
            if self._is_subgraph(path):
                compatible_paths.append(path)

        return compatible_paths


def print_suites(paths: List[Dict[str, str]]):
    if not paths:
        print("No compatible suites found.")
        return

    print(f"\nFound {len(paths)} compatible suite(s):\n")
    for i, path in enumerate(paths, 1):
        print(f"Test suite {i}:")
        for component, version in sorted(path.items()):
            print(f"  {component:20s}: {version}")
        print()


def format_compat_table(registry, pivot_component: str) -> str:
    components = sorted(registry.get_components())
    components.remove(pivot_component)
    versions = sorted(
        registry.get_component_versions(pivot_component),
        key=cmp_to_key(_compare_versions),
        reverse=True,
    )

    def cell_text(constraint):
        if constraint is None or constraint == "—":
            return "—"
        if isinstance(constraint, list):
            return ", ".join(str(x) for x in constraint)

        return str(constraint).strip()

    lines = [f"# {pivot_component}", ""]
    for ver in versions:
        lines.append(f'% cut "**{ver}**" %')
        lines.append("")
        lines.append("| component | requirements |")
        lines.append("|-----------|------------|")
        constraints = registry.get_constraints(pivot_component, ver) or {}
        for other in components:
            c = constraints.get(other, "—")
            t = cell_text(c)
            cell = f"`{t}`" if t != "—" else "—"
            lines.append(f"| {other} | {cell} |")
        lines.append("% endcut %")
        lines.append("")

    return "\n".join(lines) + "\n"
