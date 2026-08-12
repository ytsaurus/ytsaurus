# Examples of tasks for {{product-name}} Flow

This section lists task descriptions that you can solve efficiently and conveniently with {{product-name}} Flow.

{% note tip %}

The page is actively being updated. If your task (or a sufficiently similar one) isn’t listed here — {% if audience == "internal" %}write to the [YT Flow Public](https://nda.ya.ru/t/hcJkQdBD7LNa9V) chat or {% endif %}create a PR with its description (use the pencil icon in the upper-right corner).

{% endnote %}

## Incremental update of various objects’ states

Examples of such services are BigB, CaeSaR, and HitMaster.

The input is multiple (tens) of different logs; each line contains the `ProfileID` key.

Your goal is to maintain the current `ProfileID` => `Profile` state by defining functions that apply the lines to the profile. You also need to make batch requests to external systems (both synchronously and asynchronously), including requests that pass the profiles themselves.

The input log stream is 1–3 GB/s, and the state size is about 100 TB.

## Preparing logs for ML

{% if audience == "internal" %}An example service is Colibri.{% endif %}

Your goal is to build a log of the form `(is_click, factors)` by joining logs with factors and an ad event on `JoinID`.

- Only clicks/impressions within an hour after the hit are considered (a time-window join).
- You need all clicks (they account for a few percent of all events); you must sample impressions to reach a count roughly comparable to the number of clicks.
- The input factor stream is 15–20 GB/s in compressed form.

## Robot list

This is a direct analogue of the AA2 task, illustrated by the BigB task.

There is a stream of requests to the service (2 million [RPS](*rps), about 10 million [KPS](*kps)).

Your goal is to show the top 100 requested keys in real time.

## RT ABT

There is an event stream (~1 million RPS). From each event, you extract up to 10 incremental metrics, up to 100 test IDs (experiments), and up to 10 slices.

Your goal is to output a dynamic table [`TestID`, `TimeWindow(5min)`, `Slice`, `Metric`, `Value`] for efficient metric value queries.

## RT Autobudget

{% if audience == "internal" %}As a component of [CaeSaR](https://nda.ya.ru/t/F2x4cUph7gKZHn).{% endif %}

Autobudget manages a campaign so that its metrics (spent budget, cost per conversion, etc.) meet the targets set by the advertiser. For example, if a campaign has too few impressions, Autobudget increases the bid. If impressions, clicks, or conversions are excessive or unreasonably expensive, it lowers the bid.

To do this, it’s critical to understand the completeness of the processed logs: at a minimum, you must distinguish between no events for a campaign and no logs.

In addition, for effective prediction, data from different sources must be consistent with each other — that is, they must contain information for the same point in time. If the profile shows the number of clicks up to 13:00 and the number of conversions up to 12:30, that’s worse than having information in the profile for exactly 12:30.

## RT Antifraud

The system processes an event log and tags it with fraud flags as output.

Key features:

- You can describe rules in a YQL-like syntax.
- You calculate statistics for about ten keys.
- To make a decision on a single event, you need to look “into the future” by at least a minute (to block even the first click in a fraud series).


## See also

- [About Flow](../../flow/about.md)
- [Getting started](../../flow/start.md)
{% if audience == "internal" %}- [Who uses Flow](../../yandex-specific/flow/other/framework_users.md){% endif %}

[*rps]: Requests per second
[*kps]: Keys per second