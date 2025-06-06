# Histogram

Набор вспомогательных функций для [агрегатной функции HISTOGRAM](../../builtins/aggregation.md). В описании сигнатур ниже под HistogramStruct подразумевается результат работы агрегатной функции `HISTOGRAM`, `LinearHistogram` или `LogarithmicHistogram`, который является структурой определенного вида.

## Список функций

* `Histogram::Print(HistogramStruct{Flags:AutoMap}, Byte?) -> String`
* `Histogram::Normalize(HistogramStruct{Flags:AutoMap}, [Double?]) -> HistogramStruct` &mdash; во втором аргументе желаемая площадь гистограммы, по умолчанию 100.
* `Histogram::ToCumulativeDistributionFunction(HistogramStruct{Flags:AutoMap}) -> HistogramStruct`
* `Histogram::GetSumAboveBound(HistogramStruct{Flags:AutoMap}, Double) -> Double`
* `Histogram::GetSumBelowBound(HistogramStruct{Flags:AutoMap}, Double) -> Double`
* `Histogram::GetSumInRange(HistogramStruct{Flags:AutoMap}, Double, Double) -> Double`
* `Histogram::CalcUpperBound(HistogramStruct{Flags:AutoMap}, Double) -> Double`
* `Histogram::CalcLowerBound(HistogramStruct{Flags:AutoMap}, Double) -> Double`
* `Histogram::CalcUpperBoundSafe(HistogramStruct{Flags:AutoMap}, Double) -> Double`
* `Histogram::CalcLowerBoundSafe(HistogramStruct{Flags:AutoMap}, Double) -> Double`

У `Histogram::Print` есть опциональный числовой аргумент, который задает максимальную длину столбцов гистограммы (в символах, так как гистограмма рисуется в технике ASCII-арт). Значение по умолчанию &mdash; 25. Данная функция предназначена в первую очередь для просмотра гистограмм в консоли. Веб-интерфейс автоматически делает их интерактивную визуализацию.
{% if audience == "internal" %}

[Примеры](https://a.yandex-team.ru/arc/trunk/arcadia/yql/essentials/udfs/common/histogram/test/cases/).
{% endif %}
