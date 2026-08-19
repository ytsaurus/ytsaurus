package tech.ytsaurus.flow.pipeline;

import java.util.ArrayList;
import java.util.List;

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
    @Parameter(
            names = "--validate-only",
            description = "Validate the pipeline spec and exit without side effects; passed through to flow_server"
    )
    private boolean validateOnly;
    @Parameter(
            names = "--skip-set-flow-core-target",
            description = "Do not update the FlowCoreTarget for this launch; passed through to flow_server"
    )
    private boolean skipSetFlowCoreTarget;
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

    public boolean isValidateOnly() {
        return validateOnly;
    }

    public void setValidateOnly(boolean validateOnly) {
        this.validateOnly = validateOnly;
    }

    public boolean isSkipSetFlowCoreTarget() {
        return skipSetFlowCoreTarget;
    }

    public void setSkipSetFlowCoreTarget(boolean skipSetFlowCoreTarget) {
        this.skipSetFlowCoreTarget = skipSetFlowCoreTarget;
    }

    /**
     * The flags {@code flow_server} accepts on its own command line, in the form they are passed
     * through.
     *
     * @return the pass-through flags this command line selected.
     */
    public List<String> getFlowServerFlags() {
        List<String> flags = new ArrayList<>();
        if (validateOnly) {
            flags.add("--validate-only");
        }
        if (skipSetFlowCoreTarget) {
            flags.add("--skip-set-flow-core-target");
        }
        return flags;
    }
}
