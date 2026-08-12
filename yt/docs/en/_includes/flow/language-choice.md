## Choose a language {#choose-language}

Flow supports five languages for implementing business logic:

- **[C++](../../flow/cpp/getting-started.md)** — native implementation, maximum performance, full control. Use this for high-load pipelines.
- **[Java and Kotlin](../../flow/java/getting-started.md)** — run via the [companion](../../flow/concepts/companion.md) mechanism. They support Spring Boot. These are suitable for teams with a JVM stack.
- **[Python](../../flow/python/getting-started.md)** — runs via the [companion](../../flow/concepts/companion.md) mechanism. This is the easiest way to prototype a pipeline or process a small data stream.
- **[Go](../../flow/go/getting-started.md)** — runs via the [companion](../../flow/concepts/companion.md) mechanism. A single binary runs the pipeline and acts as a companion in the job. Suitable for teams with a Go stack.
- **[YQL](../../flow/yql/getting-started.md)** — declarative pipeline description as an SQL query. It has a low entry barrier and doesn’t require writing code in C++, Java, Kotlin, Go, or Python. It’s under active development, and not all planned features are available yet.