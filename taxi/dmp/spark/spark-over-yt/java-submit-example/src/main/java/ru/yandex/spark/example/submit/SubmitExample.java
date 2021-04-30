package ru.yandex.spark.example.submit;

import org.apache.spark.deploy.rest.DriverState;
import org.apache.spark.launcher.InProcessLauncher;

import ru.yandex.spark.yt.submit.SubmissionClient;
import ru.yandex.spark.yt.wrapper.client.DefaultRpcCredentials;

public class SubmitExample {
    public static void main(String[] args) {
        SubmissionClient client = new SubmissionClient(
                "hahn",
                "//home/taxi-dwh-dev/test/spark-discovery-3",
                "1.5.1",
                DefaultRpcCredentials.user(),
                DefaultRpcCredentials.token()
        );

        InProcessLauncher launcher = client.newLauncher()
                .setAppResource("yt:///sys/spark/examples/spark-over-yt-examples-jar-with-dependencies.jar")
                .setMainClass("ru.yandex.spark.example.SmokeTest");

        String submissionId = client.submit(launcher);
        DriverState state = client.getStatus(submissionId);

        System.out.println(state);

        while (!state.isFinal()) {
            System.out.println(state);
            state = client.getStatus(submissionId);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        System.out.println(state);
        System.out.println(state.isSuccess());
        System.out.println(state.isFailure());
    }
}
