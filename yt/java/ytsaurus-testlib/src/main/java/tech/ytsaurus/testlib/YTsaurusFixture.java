package tech.ytsaurus.testlib;

import java.time.Duration;

import tech.ytsaurus.client.HostPort;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.YTsaurusClientConfig;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.RemoveNode;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;

public class YTsaurusFixture {
    final HostPort address;
    final YTsaurusClient yt;
    final YPath testDirectory;

    private YTsaurusFixture(Builder builder) {
        String javaHome = builder.isContainerRunning ? "/opt/jdk11" : System.getProperty("java.home");

        var yt = YTsaurusClient.builder()
                .setCluster(builder.ytsaurusAddress)
                .setConfig(
                        YTsaurusClientConfig.builder()
                                .setRpcOptions(builder.rpcOptions)
                                .setJavaBinary(javaHome + "/bin/java")
                                .setJobSpecPatch(null)
                                .setSpecPatch(null)
                                .setOperationPingPeriod(Duration.ofMillis(500))
                                .build()
                )
                .setAuth(
                        YTsaurusClientAuth.builder()
                                .setUser("root")
                                .setToken("")
                                .build()
                )
                .build();

        yt.createNode(
                CreateNode.builder()
                        .setPath(builder.testDirectoryPath)
                        .setType(CypressNodeType.MAP)
                        .setRecursive(true)
                        .setForce(true)
                        .build()
        ).join();

        this.address = HostPort.parse(builder.ytsaurusAddress);
        this.yt = yt;
        this.testDirectory = builder.testDirectoryPath;
    }

    public YTsaurusClient getYt() {
        return yt;
    }

    public YPath getTestDirectory() {
        return testDirectory;
    }

    public HostPort getAddress() {
        return address;
    }

    public void stop() {
        var rpcRequestsTestingController = yt.getConfig().getRpcOptions()
                .getTestingOptions().getRpcRequestsTestingController();
        if (rpcRequestsTestingController != null) {
            var transactionIds = rpcRequestsTestingController.getStartedTransactions();
            for (GUID transactionId : transactionIds) {
                try {
                    yt.abortTransaction(transactionId).join();
                } catch (Exception ignored) {
                }
            }
        }
        yt.removeNode(RemoveNode.builder().setPath(testDirectory).setForce(true).build()).join();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String ytsaurusAddress;
        private boolean isContainerRunning;
        private RpcOptions rpcOptions = new RpcOptions();
        private YPath testDirectoryPath;

        public Builder setYTsaurusAddress(String ytsaurusAddress) {
            this.ytsaurusAddress = ytsaurusAddress;
            return this;
        }

        public Builder setContainerRunning(boolean containerRunning) {
            isContainerRunning = containerRunning;
            return this;
        }

        public Builder setRpcOptions(RpcOptions rpcOptions) {
            this.rpcOptions = rpcOptions;
            return this;
        }

        public Builder setTestDirectoryPath(YPath testDirectoryPath) {
            this.testDirectoryPath = testDirectoryPath;
            return this;
        }

        public YTsaurusFixture build() {
            return new YTsaurusFixture(this);
        }
    }
}
