"""
..
  PYTEST_DONT_REWRITE
"""

import operator
from math import isinf

from .utils import report_online_progress
from .utils import report_progress

NUMBER_FMT = '{0:,.4f}'
ALIGNED_NUMBER_FMT = '{0:>{1},.4f}{2:<{3}}'
STAT_PROPS = ('min', 'max', 'mean', 'median', 'iqr', 'stddev', 'ops')
DELTA = '\N{GREEK CAPITAL LETTER DELTA}'


def compute_best_worst(benchmarks, progress_reporter, tr, line):
    worst = {}
    best = {}
    for line1, prop in progress_reporter(STAT_PROPS, tr, '{line}: {value}', line=line):
        # For 'ops', higher is better; for time-based metrics, lower is better
        best_fn, worst_fn = (max, min) if prop == 'ops' else (min, max)
        values = progress_reporter(benchmarks, tr, '{line} ({pos}/{total})', line=line1)
        best[prop] = best_fn(bench[prop] for _, bench in values)
        values = progress_reporter(benchmarks, tr, '{line} ({pos}/{total})', line=line1)
        worst[prop] = worst_fn(bench[prop] for _, bench in values)
    return best, worst


class TableResults:
    def __init__(self, columns, sort, histogram, name_format, logger, scale_unit):
        self.columns = columns
        self.sort = sort
        self.histogram = histogram
        self.name_format = name_format
        self.logger = logger
        self.scale_unit = scale_unit

    def compute_scale(self, benchmarks, best, worst):
        unit, adjustment = self.scale_unit(unit='seconds', benchmarks=benchmarks, best=best, worst=worst, sort=self.sort)
        ops_unit, ops_adjustment = self.scale_unit(unit='operations', benchmarks=benchmarks, best=best, worst=worst, sort=self.sort)
        labels = {
            'name': f'Name (time in {unit}s)',
            'min': 'Min',
            'max': 'Max',
            'mean': 'Mean',
            'stddev': 'StdDev',
            'rounds': 'Rounds',
            'iterations': 'Iterations',
            'iqr': 'IQR',
            'median': 'Median',
            'outliers': 'Outliers',
            'ops': f'OPS ({ops_unit}ops/s)' if ops_unit else 'OPS',
        }
        return unit, adjustment, ops_adjustment, labels

    def display(self, tr, groups, progress_reporter=report_progress):
        tr.write_line('')
        report_online_progress(progress_reporter, tr, 'Computing stats ...')
        for line, (group, benchmarks) in progress_reporter(groups, tr, 'Computing stats ... group {pos}/{total}'):
            benchmarks = sorted(benchmarks, key=operator.itemgetter(self.sort))
            for bench in benchmarks:
                bench['name'] = self.name_format(bench)

            solo = len(benchmarks) == 1
            best, worst = compute_best_worst(benchmarks, progress_reporter, tr, line)
            for line1, prop in progress_reporter(('outliers', 'rounds', 'iterations'), tr, '{line}: {value}', line=line):
                worst[prop] = max(
                    benchmark[prop] for _, benchmark in progress_reporter(benchmarks, tr, '{line} ({pos}/{total})', line=line1)
                )

            unit, adjustment, ops_adjustment, labels = self.compute_scale(benchmarks, best, worst)
            widths = {
                'name': 3 + max(len(labels['name']), max(len(benchmark['name']) for benchmark in benchmarks)),
                'rounds': 2 + max(len(labels['rounds']), len(str(worst['rounds']))),
                'iterations': 2 + max(len(labels['iterations']), len(str(worst['iterations']))),
                'outliers': 2 + max(len(labels['outliers']), len(str(worst['outliers']))),
                'ops': 2 + max(len(labels['ops']), len(NUMBER_FMT.format(best['ops'] * ops_adjustment))),
            }
            for prop in STAT_PROPS:
                if prop not in widths:
                    widths[prop] = 2 + max(len(labels[prop]), max(len(NUMBER_FMT.format(bench[prop] * adjustment)) for bench in benchmarks))

            rpadding = 0 if solo else 10
            labels_line = labels['name'].ljust(widths['name']) + ''.join(
                labels[prop].rjust(widths[prop]) + (' ' * rpadding if prop not in ['outliers', 'rounds', 'iterations'] else '')
                for prop in self.columns
            )
            report_online_progress(progress_reporter, tr, '')
            tr.write_line(
                ' benchmark{name}: {count} tests '.format(
                    count=len(benchmarks),
                    name='' if group is None else f' {group!r}',
                ).center(len(labels_line), '-'),
                yellow=True,
            )
            tr.write_line(labels_line)
            tr.write_line('-' * len(labels_line), yellow=True)

            for bench in benchmarks:
                has_error = bench.get('has_error')
                tr.write(bench['name'].ljust(widths['name']), red=has_error, invert=has_error)
                for prop in self.columns:
                    if prop in ('min', 'max', 'mean', 'stddev', 'median', 'iqr'):
                        tr.write(
                            ALIGNED_NUMBER_FMT.format(
                                bench[prop] * adjustment, widths[prop], compute_baseline_scale(best[prop], bench[prop], rpadding), rpadding
                            ),
                            green=not solo and bench[prop] == best.get(prop),
                            red=not solo and bench[prop] == worst.get(prop),
                            bold=True,
                        )
                    elif prop == 'ops':
                        tr.write(
                            ALIGNED_NUMBER_FMT.format(
                                bench[prop] * ops_adjustment,
                                widths[prop],
                                compute_baseline_scale(best[prop], bench[prop], rpadding),
                                rpadding,
                            ),
                            green=not solo and bench[prop] == best.get(prop),
                            red=not solo and bench[prop] == worst.get(prop),
                            bold=True,
                        )
                    else:
                        tr.write('{0:>{1}}'.format(bench[prop], widths[prop]))
                tr.write('\n')
            tr.write_line('-' * len(labels_line), yellow=True)
            tr.write_line('')
            if self.histogram:
                from .histogram import make_histogram  # noqa: PLC0415

                if len(benchmarks) > 75:
                    self.logger.warning(f'Group {group!r} has too many benchmarks. Only plotting 50 benchmarks.')
                    benchmarks = benchmarks[:75]

                output_file = make_histogram(self.histogram, group, benchmarks, unit, adjustment)

                self.logger.info(f'Generated histogram: {output_file}', bold=True)

        tr.write_line('Legend:')
        tr.write_line('  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.')
        tr.write_line('  OPS: Operations Per Second, computed as 1 / Mean')


class CompareBetweenResults(TableResults):
    def display(self, tr, groups, progress_reporter=report_progress):
        tr.write_line('')

        for line, (group, benchmarks) in progress_reporter(groups, tr, 'Computing stats ... group {pos}/{total}'):
            self._display_single_between(line, group, benchmarks, tr=tr, progress_reporter=progress_reporter)

        tr.write_line('Legend:')
        tr.write_line('  Cyan: reference source for comparison. Green: improvement, Red: regression.')
        tr.write_line(f'  {DELTA}: percentage change from reference source.')

    def _display_single_between(self, line, group, benchmarks, *, tr, progress_reporter):
        # Collect sources in order of first appearance and build fullname -> {source: bench} mapping
        sources = list(dict.fromkeys(bench.get('source', '') for bench in benchmarks))
        bench_map = {}
        for bench in benchmarks:
            bench_map.setdefault(bench['fullname'], {})[bench.get('source', '')] = bench

        if len(sources) < 2:
            tr.write_line(f'ERROR: --between requires at least 2 source files, got {len(sources)}.', red=True)
            return

        metrics = [c for c in self.columns if c in STAT_PROPS]

        all_benches = list(benchmarks)
        best, worst = compute_best_worst(all_benches, progress_reporter, tr, line)
        _unit, adjustment, ops_adjustment, labels = self.compute_scale(all_benches, best, worst)
        adjustments = {m: (ops_adjustment if m == 'ops' else adjustment) for m in metrics}

        # Sort benchmarks by the sort metric from the first source
        first_source = sources[0]
        sorted_names = sorted(
            bench_map,
            key=lambda name: bench_map[name].get(first_source, {}).get(self.sort, float('inf')),
        )

        # Format benchmark display names without source suffix
        display_names = {}
        for fullname in sorted_names:
            any_bench = next(iter(bench_map[fullname].values()))
            display_names[fullname] = self.name_format({**any_bench, 'source': ''})

        name_width = 3 + max(len(labels['name']), *(len(n) for n in display_names.values()))

        columns = []  # Each entry: (source_idx, metric, label, width, is_change)

        def _val_col_width(metric, src, label):
            """
            Helper to compute value column width for a given metric and source
            """
            adj = adjustments[metric]
            w = len(label)
            for fn in sorted_names:
                bench = bench_map[fn].get(src)
                if bench is not None:
                    w = max(w, len(NUMBER_FMT.format(bench[metric] * adj)))
            return w + 2

        # Format source labels using name_format
        source_labels = [self.name_format({'name': '', 'fullname': '', 'source': src}).strip(' ()') for src in sources]

        # Per-metric grouping: ref value, then for each other source: value + delta
        for metric in metrics:
            mlabel = labels[metric]
            # Reference column
            ref_label = f'{source_labels[0]} {mlabel}'
            columns.append((0, metric, ref_label, _val_col_width(metric, sources[0], ref_label), False))
            # Other sources with deltas
            for si in range(1, len(sources)):
                val_label = f'{source_labels[si]} {mlabel}'
                columns.append((si, metric, val_label, _val_col_width(metric, sources[si], val_label), False))
                diff_label = f'{DELTA}{mlabel}'
                columns.append((si, metric, diff_label, max(len(diff_label) + 2, 10), True))

        header = ''.join(
            [
                labels['name'].ljust(name_width),
                *(label.rjust(width) for _si, _metric, label, width, _is_change in columns),
            ]
        )

        group_name = '' if group is None else f' {group!r}'
        tr.write_line(
            f' benchmark{group_name}: {len(sorted_names)} tests, {len(sources)} sources '.center(len(header), '-'),
            yellow=True,
        )
        tr.write_line(header)
        tr.write_line('-' * len(header), yellow=True)

        for fullname in sorted_names:
            tr.write(display_names[fullname].ljust(name_width))
            row_values = {}
            for si, metric, _label, width, is_change in columns:
                if not is_change:
                    bench = bench_map[fullname].get(sources[si])
                    if bench is None:
                        tr.write('N/A'.rjust(width))
                    else:
                        row_values[(si, metric)] = bench[metric]
                        tr.write(NUMBER_FMT.format(bench[metric] * adjustments[metric]).rjust(width), cyan=(si == 0))
                else:
                    base_val = row_values.get((0, metric))
                    cmp_val = row_values.get((si, metric))
                    if base_val is None or cmp_val is None or base_val == 0:
                        tr.write('N/A'.rjust(width))
                    else:
                        pct = (cmp_val - base_val) / abs(base_val) * 100
                        # For ops, higher is better; for time metrics, lower is better
                        is_improvement = (pct > 0) if metric == 'ops' else (pct < 0)
                        is_regression = (pct < 0) if metric == 'ops' else (pct > 0)
                        tr.write(f'{pct:+.1f}%'.rjust(width), green=is_improvement, red=is_regression, bold=True)
            tr.write('\n')

        tr.write_line('-' * len(header), yellow=True)
        tr.write_line('')


def compute_baseline_scale(baseline, value, width):
    if not width:
        return ''
    if value == baseline:
        return ' (1.0)'.ljust(width)

    scale = abs(value / baseline) if baseline else float('inf')
    if scale > 1000:
        if isinf(scale):
            return ' (inf)'.ljust(width)
        else:
            return ' (>1000.0)'.ljust(width)
    else:
        return f' ({scale:.2f})'.ljust(width)
