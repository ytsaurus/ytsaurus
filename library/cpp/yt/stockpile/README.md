# stockpile

As the memory limit in cgroup approaches, Linux triggers the direct reclaim mechanism to free up available memory. However, according to YT's experience, direct reclaim significantly slows down the entire process.

The problem does not only arise when memory is occupied by anonymous pages. 50% of a container's memory can be occupied by non-dirty pages in page cache, and the issue will still manifest. For example, if a process actively reads files from disk without O_DIRECT, memory fills up very quickly.

To overcome this problem, a hook `madvise(MADV_STOCKPILE)` has been added to the Yandex kernel. More details are available at https://ytsaurus.tech/internal/N2larTtt7Kzty9
