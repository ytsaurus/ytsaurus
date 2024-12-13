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
void TServerProgram::DoRun()
{
    TThread::SetCurrentThreadName(MainThreadName_.c_str());
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
