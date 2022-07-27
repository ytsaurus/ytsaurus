#include "bootstrap.h"
#include "config.h"

#include <yt/yt/ytlib/program/helpers.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>

#include <util/system/thread.h>

namespace NYT::NLogTailer {

////////////////////////////////////////////////////////////////////////////////

class TLogTailerProgram
    : public TProgram
    , public TProgramConfigMixin<TLogTailerBootstrapConfig>
{
public:
    TLogTailerProgram()
        : TProgramConfigMixin(Opts_, false)
    {
        Opts_.AddLongOption("monitoring-port", "ytserver monitoring port")
            .DefaultValue(10242)
            .StoreResult(&MonitoringPort_);
        Opts_.SetFreeArgsMin(0);
        Opts_.SetFreeArgsMax(1);
        Opts_.SetFreeArgTitle(0, "writer-pid");
    }

protected:
    void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        TThread::SetCurrentThreadName("LogTailerMain");

        ConfigureCrashHandler();

        auto config = GetConfig();
        config->MonitoringPort = MonitoringPort_;
        if (parseResult.GetFreeArgCount() == 1) {
            auto freeArgs = parseResult.GetFreeArgs();
            config->LogTailer->LogRotation->LogWriterPid = FromString<int>(freeArgs[0]);
        }

        ConfigureNativeSingletons(config);
        StartDiagnosticDump(config);

        TBootstrap bootstrap(std::move(config));
        bootstrap.Run();
    }

private:
    int MonitoringPort_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
