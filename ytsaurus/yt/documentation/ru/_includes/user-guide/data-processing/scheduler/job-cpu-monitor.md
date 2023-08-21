# Динамический учёт потребления CPU

В случае, когда джоб потребляет существенно меньше cpu, чем было заказано в спецификации операции, гарантия CPU джоба может быть уменьшена. Освободившиеся ресурсы могут быть переданы другим джобам, работающим в операциях конкретного пула. За потреблением и изменением гарантий CPU джобов следит процесс `job-cpu-monitor`.


Периодически с контейнера с джобом снимается значение потребленного CPU за период (`check_period`). К этому значению применяется экспоненциальное сглаживание с параметром `smoothing_factor`. Далее рассматриваются последние `vote_window_size` сглаженных значений и интервал (`relative_lower_bound*current_cpu_limit`, `relative_upper_bound*current_cpu_limit`), где current_cpu_limit — текущий лимит, выставленный на контейнере. 
После этого каждое значение преобразуется по правилу:

* `-1` — значение < `relative_lower_bound*current_cpu_limit`;
* `1` — значение > `relative_upper_bound*current_cpu_limit`;
* `0` — в остальных случаях.

Полученные значения суммируются в переменную `votes_sum`, текущее ограничение `current_cpu_limit` пересчитывается:

* `votes_sum > votes_decision_threshold => current_cpu_limit *= increase_coefficient`
* `votes_sum < -votes_decision_threshold => current_cpu_limit *= decrease_coefficient`

Значение `current_cpu_limit` ограничено снизу опцией `min_cpu_limit` из конфигурации `job-cpu-monitor` и сверху опцией `cpu_limit` из спецификации операции.
В случае, если значение переменной current_cpu_limit изменилось, новое значение выставляется на контейнер и отправляется планировщику, чтобы актуализировать потребление ресурсов в пуле.

`job-cpu-monitor` стремится держать текущее потребление CPU в интервале между `relative_lower_bound` и `relative_upper_bound` от выставленного на контейнере, и сдвигает указанный интервал в большую или меньшую сторону, если потребление выходит за его границы.

Значения по умолчанию (актуальные могут отличаться):

* `check_period = 1000` (ms);
* `smoothing_factor = 0.1`;
* `relative_upper_bound = 0.9`;
* `relative_lower_bound = 0.6`;
* `increase_coefficient = 1.45`;
* `decrease_coefficient = 0.97`;
* `vote_window_size = 5`;
* `vote_decision_threshold = 3`;
* `min_cpu_limit = 1`.

Перечисленные настройки можно задать в спецификации операции в секции `job-cpu-monitor`. В секции можно указать опцию `enable_cpu_reclaim`, которая включает/отключает изменение ограничения cpu. Актуальные значения опций можно посмотреть в веб-интерфейсе на странице операции, на вкладке `Specification` -> `Resulting specification` -> `job-cpu-monitor`.