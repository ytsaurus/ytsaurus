# Dynamic monitoring of CPU consumption

If a job consumes significantly fewer cpu resources than ordered in the operation specification, the job CPU guarantee may be reduced. The freed up resources can be transferred to other jobs working in the operations of a particular pool. The `job-cpu-monitor` process monitors job CPU consumption and guarantee changes.


The value of CPU consumption for the period (`check_period`) is regularly read from the job container. Exponential smoothing with the `smoothing_factor` parameter is applied to this value. The last `vote_window_size` smoothed values and the interval (`relative_lower_bound*current_cpu_limit`, `relative_upper_bound*current_cpu_limit`) where current_cpu_limit is a current limit set on the container are considered next.
After that, each value is converted according to the rule:

* `-1`: Value < `relative_lower_bound*current_cpu_limit`.
* `1`: Value > `relative_upper_bound*current_cpu_limit`.
* `0`: In other cases.

The resulting values are summed into the `votes_sum` variable and the `current_cpu_limit` limit is recalculated:

* `votes_sum > votes_decision_threshold => current_cpu_limit *= increase_coefficient`
* `votes_sum < -votes_decision_threshold => current_cpu_limit *= decrease_coefficient`

The `current_cpu_limit` value is limited at the bottom by the `min_cpu_limit` option from the `job-cpu-monitor` configuration and at the top by the `cpu_limit` option from the operation specification.
If the value of the current_cpu_limit variable has changed, the new value is set on the container and sent to the scheduler to update resource consumption in the pool.

`Job-cpu-monitor` aims to keep the current CPU consumption in the interval between `relative_lower_bound` and `relative_upper_bound` from that set on the container and shifts the specified interval up or down if the consumption exceeds its limits.

The default values (the current values may be different):

* `check_period = 1000` (ms);
* `smoothing_factor = 0.1`;
* `relative_upper_bound = 0.9`;
* `relative_lower_bound = 0.6`;
* `increase_coefficient = 1.45`;
* `decrease_coefficient = 0.97`;
* `vote_window_size = 5`;
* `vote_decision_threshold = 3`;
* `min_cpu_limit = 1`.

The listed settings can be specified in the `job-cpu-monitor` section of the operation specification. In the section, you can specify the `enable_cpu_reclaim` option that enables/disables cpu limit changes. You can view the actual option values in the web interface on the operation page, on the `Specification` -> `Resulting specification` -> `job-cpu-monitor` tab.