#include "server_program.h"

#include <util/system/thread.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TServerProgram::TServerProgram()
    : TProgram()
    , TProgramPdeathsigMixin(Opts_)
    , TProgramSetsidMixin(Opts_)
{ }

void TServerProgram::SetMainThreadName(const std::string& name)
{
    MainThreadName_ = name;
}

const NLastGetopt::TOptsParseResult& TServerProgram::GetOptsParseResult() const
{
    return *OptsParseResult_;
}

void TServerProgram::DoRun(const NLastGetopt::TOptsParseResult& parseResult)
{
    TThread::SetCurrentThreadName(MainThreadName_.c_str());
    // TODO(babenko): maybe stop passing opts in DoRun? push GetOptsParseResult down to TProgram?
    OptsParseResult_ = &parseResult;
    RunMixinCallbacks();
    Configure();
    DoStart();
}

void TServerProgram::Configure()
{
    ConfigureUids();
    ConfigureIgnoreSigpipe();
    ConfigureCrashHandler();
    ConfigureExitZeroOnSigterm();
    EnablePhdrCache();
    ConfigureAllocator();
    MlockFileMappings();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
