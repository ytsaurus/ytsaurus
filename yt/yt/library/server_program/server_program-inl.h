#ifndef SERVER_PROGRAM_INL_H_
#error "Direct inclusion of this file is not allowed, include server_program.h"
// For the sake of sane code completion
#include "server_program.h"
#endif

#include "config.h"

#include <util/system/thread.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TConfig, class TDynamicConfig>
TServerProgram<TConfig, TDynamicConfig>::TServerProgram()
    : TServerProgramBase()
    , TProgramPdeathsigMixin(Opts_)
    , TProgramSetsidMixin(Opts_)
    , TProgramConfigMixin<TConfig, TDynamicConfig>(Opts_)
{ }

template <class TConfig, class TDynamicConfig>
void TServerProgram<TConfig, TDynamicConfig>::DoRun()
{
    TThread::SetCurrentThreadName(GetMainThreadName().c_str());

    RunMixinCallbacks();

    ValidateOpts();

    TweakConfig();

    this->Configure(this->GetConfig());

    DoStart();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
