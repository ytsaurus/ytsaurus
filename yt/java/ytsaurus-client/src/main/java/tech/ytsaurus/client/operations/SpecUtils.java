package tech.ytsaurus.client.operations;


import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.TransactionalOptions;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;


@NonNullApi
@NonNullFields
final class SpecUtils {
    private SpecUtils() {
    }

    static YTreeBuilder addMapperOrReducerTitle(YTreeBuilder builder, UserJobSpec mapperOrReducerSpec) {
        if (mapperOrReducerSpec instanceof MapperOrReducerSpec) {
            return builder.key("title").value(
                    ((MapperOrReducerSpec) mapperOrReducerSpec).getMapperOrReducerTitle());
        } else if (mapperOrReducerSpec instanceof CommandSpec) {
            return builder.key("title").value(
                    ((CommandSpec) mapperOrReducerSpec).getCommand());
        } else {
            return builder;
        }
    }

    static Optional<String> getMapperOrReducerTitle(UserJobSpec mapperOrReducerSpec) {
        if (mapperOrReducerSpec instanceof MapperOrReducerSpec) {
            return Optional.of(((MapperOrReducerSpec) mapperOrReducerSpec).getMapperOrReducerTitle());
        } else if (mapperOrReducerSpec instanceof CommandSpec) {
            return Optional.of(((CommandSpec) mapperOrReducerSpec).getCommand());
        } else {
            return Optional.empty();
        }
    }

    static YTreeBuilder startedBy(YTreeBuilder builder, SpecPreparationContext context) {
        YTreeBuilder result = builder.beginMap()
                .key("user").value(System.getProperty("user.name"))
                .key("command").beginList().value("command").endList()
                .key("hostname").value(getLocalHostname())
                .key("wrapper_version").value(context.getConfiguration().getVersion());
        try {
            result = result.key("pid").value(getPid());
        } catch (RuntimeException ignored) {
        }
        return result.endMap();
    }

    private static int getPid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        int index = name.indexOf('@');
        if (index == -1) {
            throw new RuntimeException("failed to get PID using JVM MX Beans");
        }

        try {
            return Integer.parseInt(name.substring(0, index));
        } catch (NumberFormatException e) {
            throw new RuntimeException("cannot parse PID from " + name);
        }
    }

    private static String getLocalHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
    }

    static void createOutputTables(
            TransactionalClient yt,
            @Nullable TransactionalOptions transactionalOptions,
            List<YPath> outputTables,
            Map<String, YTreeNode> outputTableAttributes
    ) {
        for (YPath outputTable : outputTables) {
            yt.createNode(CreateNode.builder()
                    .setPath(outputTable)
                    .setType(CypressNodeType.TABLE)
                    .setAttributes(outputTableAttributes)
                    .setRecursive(true)
                    .setIgnoreExisting(true)
                    .setTransactionalOptions(transactionalOptions)
                    .build()).join();
        }
    }
}
