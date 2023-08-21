#include "config.h"

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

void TJobTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("delay_after_node_directory_prepared", &TThis::DelayAfterNodeDirectoryPrepared)
        .Default();
    registrar.Parameter("delay_in_cleanup", &TThis::DelayInCleanup)
        .Default();
    registrar.Parameter("fail_before_job_start", &TThis::FailBeforeJobStart)
        .Default(false);
    registrar.Parameter("throw_in_shallow_merge", &TThis::ThrowInShallowMerge)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
