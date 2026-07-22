package tech.ytsaurus.flow.pipeline;

import com.beust.jcommander.Parameter;
import org.jspecify.annotations.Nullable;

public class FlowCliArguments {
    @Parameter(
            names = "--config",
            description = "Config path",
            required = true
    )
    private @Nullable String configPath;
    @Parameter(
            names = "--flow-bin",
            description = "Path to the flow_server binary; the runner enriches the spec and hands "
                    + "the launch off to flow_server, which sets the spec and starts the pipeline",
            required = true
    )
    private @Nullable String flowBin;
    @Parameter(names = "--help", help = true)
    private boolean help;

    public @Nullable String getConfigPath() {
        return configPath;
    }

    public void setConfigPath(String configPath) {
        this.configPath = configPath;
    }

    public @Nullable String getFlowBin() {
        return flowBin;
    }

    public void setFlowBin(String flowBin) {
        this.flowBin = flowBin;
    }

    public boolean isHelp() {
        return help;
    }

    public void setHelp(boolean help) {
        this.help = help;
    }
}
