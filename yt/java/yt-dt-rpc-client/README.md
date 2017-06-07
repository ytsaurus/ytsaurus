YT DT RPC-client (the clone of https://github.yandex-team.ru/yt/yt-dt-rpc-client, author: valri@)
=======
RPC-client for YT Dynamic Tables based on [Netty 4.1](https://github.com/netty/netty) framework.

Building
--------
Gradle used for dependency management and building purposes.
```
$ ./gradlew clean build             # builds all code
$ ./gradlew publish                 # publishes snapshot to artifactory
$ ./gradlew publishToMavenLocal     # publishes project to local mvn repository
$ ./gradlew jmh                     # run jmh profiler
```
Use `--debug --stacktrace` flags to get more information from Gradle build.

Example
--------
A stand-alone example project is located under [example](https://github.yandex-team.ru/valri/yt-dt-rpc-client/tree/master/example). Run `./gradlew build` under that directory to test it out.

Dependency 
------
Add dependency to your project. Using Gradle:
```
buildscript {
    repositories {
        maven {
            url 'http://artifactory.yandex.net/yandex_common_snapshots/'
        }
        jcenter()
    }
}

compile group: 'ru.yandex.yt', name: 'yt-rpc-client', version: '0.0.1-SNAPSHOT'
```

Basic usage
------
Create a client instance and call simple method `GetNode` for YT node info retrieval.
```
YtDtClient client = new YtDtClient(new InetSocketAddress("barney.yt.yandex.net", 9013), 
                                   YT_TOKEN, YT_LOGIN, "yt.yandex.net", rpcProtocolVersion);
try {
    System.out.println(client.getNode("//@"));
} finally {
    client.close();
}
```
Asynchronous usage
------
```
YtDtAsyncClient client = new YtDtAsyncClient(new InetSocketAddress("barney.yt.yandex.net", 9013), 
                                             YT_TOKEN, YT_LOGIN, "yt.yandex.net", rpcProtocolVersion);
CompletableFuture<String> future = client.getNode("//@");
future.whenCompleteAsync((result, throwable) -> {
    if (throwable == null) {
        System.out.println(result);
    } else {
        System.out.println("Failed to get request", throwable);
    }
    try {
        client.close();
    } catch (InterruptedException e) {
        e.printStackTrace();
    }
});
```
