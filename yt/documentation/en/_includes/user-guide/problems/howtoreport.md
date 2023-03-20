# Reporting a problem

This section describes how and in what cases to ask for help when working with {{product-name}}.  It also gives recommendations for how to compose requests and report errors.

## Request examples

{{product-name}} consists of different components and services, so there are several ways to ask for help or a question.  For each situation, there is a best way to report a problem or ask for help working with the system.  Before submitting your first request, select the appropriate method to minimize expenses in routing your request to people who can help.

Select the section that best fits your situation.

### Questions about the web interface

If you're experiencing problems with the layout, updating data on the web interface, and similar problems, write to the [ui@ytsaurus.tech](mailto:ui@ytsaurus.tech) mailing list.
Make sure to describe the problem and the expected behavior, and include the address of the page.

### Questions about the current functioning of the system

Questions on the current functioning of {{product-name}} include, but are not limited to:

- Slow reading/recording of data and long calculations on the cluster.
- Unknown errors when performing an operation or requests to the cluster.

Send a description of the problem and the most basic example necessary for the team to reproduce the problem to the [dev@ytsaurus.tech](mailto:dev@ytsaurus.tech) mailing list.
If the problem has to do with the operation's jobs being slow, first take all possible steps to diagnose the problem yourself using [Job Shell](../../../user-guide/problems/jobshell-and-slowjobs.md) and [jobs statistics](../../../user-guide/problems/jobstatistics.md).

### Questions about development

This category includes questions on the technology in general, and any consultations:

- API and its use.
- Features and restrictions.
- Questions on ways to optimize calculations.
- Questions on types of {{product-name}} quotes and how to get them.
- Questions on assigning metrics and correctly interpreting graphs.
- Questions on rating necessary resources.
- Questions about reasons errors may appear and ways to self-diagnose them.
- Conceptual questions and suggestions.

Ask such questions on [Stack Overflow](https://stackoverflow.com), and tag [ytsaurus](https://stackoverflow.com/tags/ytsaurus).

### Other questions

If none of the described categories fits your question or problem, write to the {% if lang == ru %}[community_ru@](mailto:community_ru@ytsaurus.tech){% else %}(mailto:community@ytsaurus.tech){% endif%} mailing list.

## Suggestions for reporting errors

These suggestions will help you describe the situation in its original form, completely, without preemptive interpretations, and without missing important details.

Before sending your error report, read it over and imagine that you're the person who will be helping you. How would you start solving the problem? Is there enough information in your report to do this?

**Always show the error report in full.** Errors in {{product-name}} are hierarchical: aside from the text itself, the error has attached attributes and enclosed errors.  Do not truncate the information, but show everything that's there. If the truncation happened before printing, try to figure out why this happened.  This may be useful to you in the future. If the information is being truncated by the API you're using, write to the {% if lang == ru %}[community_ru@](mailto:community_ru@ytsaurus.tech){% else %}(mailto:community@ytsaurus.tech){% endif%} mailing list.

**Enable logging.** Most SDKs provided by {{product-name}} use a `YT_LOG_LEVEL`environment variable. Establish its value in `debug`, assemble the logs from stderr and attach them to your error report. It also makes sense to take into account the following proprietary circumstances:

- When using a Python library, you must configure logging through the `logging.getLogger('Yt').setLevel(logging.DEBUG)` logging module.

- In Java libraries, slf4j is used for logging. To enable logs, enable the `debug` level for `tech.ytsaurus` loggers.

- If the problem is reproducing when you use the local {{product-name}}, enable debug logs in it, after passing the `--enable-debug-logging` option when calling `yt_local start`.

For product processes, keep debug logs enabled all the time, with the appropriate rotation and compression configured. In many complex cases, it is impossible to establish the reason without detailed logs. {{product-name}} writes detailed logs on the server side, but if the problem is in the network interaction between the client and the server, for example, server logs are useless.

**Provide more context:**

- Make sure to provide the maximum information about your cluster.
- If the operation has concluded with an error, provide the result as an example.
- If the code falls due to exclusion, you must attach a backtrace.
- If the code concludes with an error and goes into the {%if lang == ru %}(core dump){% endif %} memory dump, you have to send a link to the memory dump so it can be downloaded and examined.

**If the problem is being reproduced, try to find the minimum example to reproduce it.** Building a minimum example that reproduces the problem often enables you to find the error in the user code. If your calculations depend on additional data (tables, local files), prepare reliable links to that data. Additional data may be needed for the {{product-name}} team to reproduce the problem on their own, if they cannot find the reason for the problem by reading the logs.

**Try to provide the initial data.** Show the problem and everything having to do with it in its raw form. The {{product-name}} team will do everything they can to understand the root cause.

**Try to separate problems.** Don't write about different problems in the same error report. Submit two reports.

**Use the tools provided by the system for independent diagnosis to their full capacity.**

**Provide more preceding history to hone performance.**

**Read the documentation, especially the FAQ** located in a separate [section](../../../faq/faq.md).

