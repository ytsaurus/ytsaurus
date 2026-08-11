# Testing in {{product-name}} Flow (C++)

{% note info %}

C++ [workers](../../../flow/concepts/glossary.md#worker) currently don't have a separate framework for unit testing computations. The main approach is to run the entire pipeline with final data sources. You can find a test example in [examples/cpp/wait_click_join]({{source-root}}/yt/yt/flow/examples/cpp/wait_click_join). If you still need to write unit tests, you can move the business logic out of the computations into separate classes and write unit tests for those classes.

{% endnote %}

{% include notitle [_](../testing-integration-body.md) %}

{% include notitle [_](../testing-test-param-body.md) %}

## See also

- [Basic release rules](../../../flow/release/basic-rules.md)
- [Testing (Java)](../../../flow/java/testing.md)
- [Testing (Python)](../../../flow/python/testing.md)
- [Quick start (C++)](../../../flow/cpp/getting-started.md)
- [Quick start (Java)](../../../flow/java/getting-started.md)
- [Quick start (Python)](../../../flow/python/getting-started.md)