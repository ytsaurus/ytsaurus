# How the fair share ratio is calculated

There is a pool tree, where the internal nodes are pools and the leaves are operations.

Each operation in the pool tree has usage and demand — resource vectors that specify the current resource consumption of the operation and the maximum possible resource consumption. These characteristics (usage and demand) are naturally summed across the tree, making it possible to refer to usage and demand of the internal nodes of the tree. In addition, a resource vector can be turned into a non-negative number by taking the ratio of the dominant vector resource to the total amount of that resource on the cluster. This makes it possible to look at the usage ratio and demand ratio in each node of the tree.

When calculating the fair share ratio for all nodes of the tree, a procedure is performed recursively to divide the fair share in a given node among its children. In the root, the division starts with a fair share equal to one.

When distributing the fair share to the children, the following task is solved: Let `F` be the fair share value to be distributed to the children: `c_1, c_2, ... c_n`. The weight of the children is marked as `w_1, w_2, ... w_n` . The children also have an upper fair share limit denoted as `u_i`. This limit is calculated based on the demand, resource granularity, set resource limits, and so on. The lower fair share limit is denoted as `l_i` (set by the min share ratio). The algorithm that divides the fair share searches for an `x` to execute:

![](../../../../../images/fair_share_formula.png){ .center }

If the total `min share ratio` of the children in the pool is greater than one, it is normalized to 1. In general, `min share ratio` is a complicated characteristic and we do not recommend setting it for nested pools. Otherwise, the task described above would have no solution.

A few examples of calculating the fair share ratio:

Let there be two pools in the root: `A` and `B`. The min share ratio of the `A` pool is 0.6 and the min share ratio of the `B` pool is 0.2. Let large operations with a very high demand run in both pools.

1. The weight of both pools is one. In this case, after calculating the fair share, the fair share ratio of the `A` pool is 0.6 and the fair share ratio of the `B` pool is 0.4.
2. The weight of the `A` pool is three and the weight of the `B` pool is one. Then the fair share ratio of the `A` pool is 0.75 and the fair share ratio of the `B` pool is 0.25.
3. The weight of both pools is one. Suppose that in addition to `A` and `B` pools there is a `C` pool that has no min share ratio and has a weight of 1. Then the fair share ratio of the `A` pool is 0.6, the fair share ratio of the `B` pool is 0.2, and the fair share ratio of the `C` pool is also 0.2.