# Intro

The tool for managing compatibility testing of YTsaurus components. Automatically generates and runs test configurations based on a compatibility graph (matrix) of component versions.

## Description

Builds a graph of compatible component configurations from constraints defined in config files. Each configuration is a set of compatible component versions that are materialized as YTsaurus clusters with test checks.

Compatibility is defined only between component pairs (no transitivity).

## Build

```bash
ya make -r yt/admin/ytsaurus_ci
```

## Commands

### View Compatibility Matrix

```bash
./yt/admin/ytsaurus_ci/ytsaurus_ci matrix
```

**Flags:**

- `--version-filter` — filter by component versions (JSON string)

**Examples:**

```bash
# View all
./yt/admin/ytsaurus_ci/ytsaurus_ci matrix

# Filter by operator version
./yt/admin/ytsaurus_ci/ytsaurus_ci matrix --version-filter '{"operator": "main"}'

# Multiple filters
./yt/admin/ytsaurus_ci/ytsaurus_ci matrix --version-filter '{"operator": ">=0.27.0", "ytsaurus": "25.3"}'
```

### Run scenario

```bash
./yt/admin/ytsaurus_ci/ytsaurus_ci  run-scenario
```

**Required arguments:**

- `--scenario` — scenario name from `configs/scenarios.yaml`
- `--git-token` — GitHub API token
- `--cloud-function-token` — Cloud Function token

**Optional arguments:**

- `--git-api-url` — GitHub API URL (default: `https://api.github.com`)
- `--version-filter` — version filter (JSON string, default: `{}`)
- `--apply` — apply changes (create task)
- `--force` — overwrite existing task
- `--verbose` — verbose output

**Examples:**

```bash
# By default task creates without applying
./yt/admin/ytsaurus_ci/ytsaurus_ci run-scenario \
  --scenario nightly-dev \
  --git-token <token> \
  --cloud-function-token <token>

# Run with apply
./yt/admin/ytsaurus_ci/ytsaurus_ci run-scenario \
  --scenario release-tests \
  --git-token <token> \
  --cloud-function-token <token> \
  --apply
```

### Reproduce Test

```bash
./yt/admin/ytsaurus_ci/ytsaurus_ci reproduce \
  --job-id <job_id> \
  --cloud-function-token <token>
```

## Graph (matrix) filtering

Filter operators: `==`, `>=`, `<=`, `>`, `<`, `main`, `&&`, or list `["v1, "v3"]`.

**Examples:**

```bash
# Exact version
./yt/admin/ytsaurus_ci/ytsaurus_ci matrix --version-filter '{"operator": "0.27.0"}'

# Comparison
./yt/admin/ytsaurus_ci/ytsaurus_ci matrix --version-filter '{"operator": ">=0.27.0"}'

# Combined
./yt/admin/ytsaurus_ci/ytsaurus_ci matrix --version-filter '{"operator": ">=0.27.0 && main"}'
```

## Configuration Files

### `configs/components.yaml`

Defines all components and their image sources.

**Structure:**

- `requirements` — path to compat file (compat-*.yaml)
- `origins` — container image sources:
  - `stable` — stable releases
  - `dev` — dev versions (nightly builds)
  - `release` — release versions with versioning

**Example:**

```yaml
ytsaurus:
  requirements: configs/compat-ytsaurus.yaml
  origins:
    stable:
      repo: ytsaurus
      container: ytsaurus
      image_tag: '^stable-(?P<version>{{ version }})-relwithdebinfo$'
```

### `configs/compat-<component_name>.yaml`

Compatibility files for each component (e.g., `compat-ytsaurus.yaml`, `compat-operator.yaml`).

**Structure:**

- `source` — image source from `components.yaml` (stable, release)
- `constraints` — compatibility constraints with other components

**Constraint formats:**

- Exact: `"0.27.0"`
- Comparison: `">=0.27.0"`, `"<=0.26.0"`
- Combined: `">=0.27.0 && main"`
- List: `["0.23.1", "0.26.0"]`

### `configs/scenarios.yaml`

**Version filters:**

- empty — all versions
- Specific value or condition — filter by version
- `main` — dev versions only (trunk)

## Test Canonization

Canonize tests with:

```bash
ya make -t -A -Z
```

## Notes

- Compatibility is pairwise only (no transitivity)
- All configurations in the graph are unique
- Each configuration contains all system components
- Use filters in `scenarios.yaml` to customize tested configurations
- All configs included in binary (need rebuild if you change some in `configs/` or `templates/`)
