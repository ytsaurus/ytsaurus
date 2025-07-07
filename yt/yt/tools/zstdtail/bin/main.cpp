#include <yt/yt/tools/zstdtail/lib/tailer.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

#include <library/cpp/yt/system/exit.h>

namespace NYT::NZstdtail {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TTailListener
    : public ITailListener
{
public:
    void OnData(TRef ref) override
    {
        Cout << ref.ToStringBuf();
    }

    void OnError(const TError& error) override
    {
       Cerr << ToString(error) << Endl;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TProgram
    : public NYT::TProgram
{
public:
    TProgram()
    {
        Opts_.SetTitle("zstdtail: a simplified version of 'tail' for zstd-compressed files");

        Opts_
            .AddCharOption('F', "follow newly appended data")
            .StoreTrue(&FollowFlag_);

        Opts_.SetFreeArgsMin(0);
        Opts_.SetFreeArgsMax(1);
    }

private:
    bool FollowFlag_ = false;

    void DoRun() override
    {
        Configure();

        const auto& parseResult = GetOptsParseResult();
        if (parseResult.GetFreeArgCount() == 0) {
            parseResult.PrintUsage();
            Exit(EProcessExitCode::ArgumentsError);
        }

        if (!FollowFlag_) {
            THROW_ERROR_EXCEPTION("Must specify '-F' option");
        }

        auto path = parseResult.GetFreeArgs()[0];
        TTailListener listener;
        RunTailer(path, &listener);
    }

    void Configure()
    {
        if (!NLogging::TLogManager::Get()->IsConfiguredFromEnv()) {
            // Log warnings to stderr unless explicitly configured.
            NLogging::TLogManager::Get()->Configure(
                NLogging::TLogManagerConfig::CreateStderrLogger(NLogging::ELogLevel::Warning));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZstdtail

int main(int argc, const char** argv)
{
    return NYT::NZstdtail::TProgram().Run(argc, argv);
}
