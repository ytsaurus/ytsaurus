# Spring Boot Starter for YT Flow
The `flow-spring-boot-starter` module provides Spring Boot auto-configuration for easy integration with Spring applications.

## Setup

1. Add the `flow-spring-boot-starter` dependency to your project.
2. Implement `ProcessFuncion`
```java
@FlowComputation(id="mapper")
public class WordCountMapper implements RowFunction {

    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        // User-defined processing logic.
    }
}
```
3. Create SpringBootApplication
```java
@SpringBootApplication
public class WordCountApplication {
    public static void main(String[] args) {
        new SpringApplicationBuilder(WordCountApplication.class)
                .run(args);
    }
}
```

5. The gRPC server will automatically start when the Spring application context is ready.

## Configuration Properties

You can configure the Flow server using Spring configuration properties in `application.yml` or `application.properties`:

```yaml
flow:
    server:
        port: 8080  # Optional, defaults to the port field in YT_FLOW_COMPANION_CONFIG
```

| Property           | Description                                             | Default                                                    |
|--------------------|---------------------------------------------------------|------------------------------------------------------------|
| `flow.server.port` | The port on which the gRPC companion server will listen | `port` field from `YT_FLOW_COMPANION_CONFIG` YSON payload |
