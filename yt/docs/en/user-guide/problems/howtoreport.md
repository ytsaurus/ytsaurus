# How to report a problem

This section describes when, where, and how to ask for help while working with the {{product-name}} system. It provides recommendations on writing support requests and bug reports.

## Using LLMs to compose requests

{% include notitle [Using LLMs to compose requests](../../_includes/user-guide/problems/howtoreport-llm-usage.md) %}

## When to submit a request {#cases-of-requests}

The {{product-name}} system consists of various components and services, so there are several ways to get help or ask a question.

For each situation, there is a best way to report a problem or ask for help with the system. Choose it before submitting your request — this will reduce the time it takes for the right specialist to receive your request.

Questions can fall into the following categories:

* [Web interface issues](#web-interface-issues)
* [System operation issues](#system-operation-issues)
* [Development questions](#development-questions)
* [Other questions](#other-questions)

### Web interface issues {#web-interface-issues}

Report layout problems, data update issues in the web interface, and other similar problems to the mailing list [ui@ytsaurus.tech](mailto:ui@ytsaurus.tech).
Be sure to describe the problem, the expected behavior, and provide the page address.

### System operation issues {#system-operation-issues}

Questions about the current operation of the {{product-name}} system include, but are not limited to:

- Slow data read/write and long computations on the cluster
- Unknown errors when running operations or making queries to the cluster

Send a description of the problem and a minimal example that allows the team to reproduce the issue to the mailing list [dev@ytsaurus.tech](mailto:dev@ytsaurus.tech).

{% note tip %}

If the problem is related to slow operation jobs, first make every effort to diagnose the problem yourself using [Job Shell](jobshell-and-slowjobs.md) and [job statistics](jobstatistics.md).

{% endnote %}

### Development questions {#development-questions}

This category includes questions about the technology in general and any consultations:

- API and its usage
- Capabilities and limitations
- Questions about ways to optimize computations
- Questions about {{product-name}} quota types and the process of obtaining them
- Questions about metric purpose and correct interpretation of graphs
- Questions about estimating required resources
- Questions about error causes and self-diagnosis methods
- Conceptual questions and suggestions

Ask such questions on [Stack Overflow](https://stackoverflow.com) with the tag [ytsaurus](https://stackoverflow.com/tags/ytsaurus).

### Other questions {#other-questions}

If your question or problem does not fit into any of the described categories, send an email to the mailing list {% if lang == ru %}[community_ru@](mailto:community_ru@ytsaurus.tech){% else %}(mailto:community@ytsaurus.tech){% endif%}.

## How to write effective bug reports {#how-to-write-effective-bug-reports}

The tips below will help you not miss important details in bug reports and describe the situation in its original form, without prior interpretation, in full detail.

### Formulating the problem {#problem-formulation}

**Before sending a bug report, reread** it and put yourself in the shoes of the person who will be helping you. Where would you start solving the problem? Is the information provided sufficient?

**Provide more background for performance debugging.**

### Supporting information {#auxiliary-information}

**Try to provide raw data.** Show the problem and everything related to it in its "raw" form — the {{product-name}} service team will make every effort to understand the root cause.

**Always show the full error message.** In {{product-name}}, errors are hierarchical: in addition to the text itself, the error has attributes and nested errors. Do not trim the information — show everything that is available. If trimming occurred before printing, try to figure out why that happened. This might be useful in the future.

If the API you are using trims the information, send an email to the mailing list {% if lang == ru %}[community_ru@](mailto:community_ru@ytsaurus.tech){% else %}[community@](mailto:community@ytsaurus.tech){% endif%}.

**Enable logging.** Most SDKs provided by {{product-name}} use the environment variable `YT_LOG_LEVEL`. Set its value to `debug`, collect logs from stderr, and attach them to the bug report. Also consider the following specific circumstances:

  - When using the Python library, configure logging through the logging module: `logging.getLogger('Yt').setLevel(logging.DEBUG)`

  - In Java libraries, slf4j is used for logging. Enable `debug` level for the `tech.ytsaurus` loggers

  - If the problem can be reproduced using local {{product-name}}, enable debug logs in it by passing the `--enable-debug-logging` option when calling `yt_local start`

{% note tip %}

For production processes, configure appropriate rotation and compression of debug logs and **do not disable them**. Detailed logs are needed to determine the cause of the problem.

{% endnote %}

The {{product-name}} system writes detailed logs on the server side, but if the problem lies, for example, in the network communication between the client and the server, the server logs are useless.

**Provide more context:**

- Be sure to share as much information about your cluster as possible
- If an operation fails with an error, provide a reproducible example
- If the code crashes due to an exception, attach a backtrace
- If the code terminates with an error and creates a core dump {%if lang == ru %}(core dump){% endif %}, provide a link to the core dump so it can be downloaded and examined

**If the problem can be reproduced, try to find a minimal reproduction example.** A minimal reproduction example often helps find an error in the user's code. If your computations depend on additional data (tables, local files), prepare reliable links to them. The {{product-name}} service team may need additional data to reproduce the problem independently if they cannot find the cause by reading logs.

### Structure and order {#structure-and-order}

**Try to separate problems.** Do not write about different issues in a single bug report. Write two separate reports.

### Self-help {#self-help}

**Make maximum use of the tools provided by the system for self-diagnosis.**

**Read the documentation, especially the FAQ**, which is located in a separate [section](../../faq/index.md).
