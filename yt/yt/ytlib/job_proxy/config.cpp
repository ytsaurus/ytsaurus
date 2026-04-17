#include "config.h"

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

void TEventsOnFsConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path)
        .Default();
    registrar.Parameter("breakpoints", &TThis::Breakpoints)
        .Default();
    registrar.Parameter("timeout", &TThis::Timeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("poll_period", &TThis::PollPeriod)
        .Default(TDuration::MilliSeconds(50));
}

////////////////////////////////////////////////////////////////////////////////

void TJobTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("delay_after_node_directory_prepared", &TThis::DelayAfterNodeDirectoryPrepared)
        .Default();
    registrar.Parameter("delay_in_cleanup", &TThis::DelayInCleanup)
        .Default();
    registrar.Parameter("delay_before_run_job_proxy", &TThis::DelayBeforeRunJobProxy)
        .Default();
    registrar.Parameter("fake_prepare_duration", &TThis::FakePrepareDuration)
        .Default();
    registrar.Parameter("delay_after_run_job_proxy", &TThis::DelayAfterRunJobProxy)
        .Default();
    registrar.Parameter("delay_before_spawning_job_proxy", &TThis::DelayBeforeSpawningJobProxy)
        .Default();
    registrar.Parameter("fail_before_job_start", &TThis::FailBeforeJobStart)
        .Default(false);
    registrar.Parameter("throw_in_shallow_merge", &TThis::ThrowInShallowMerge)
        .Default(false);
    registrar.Parameter("events_on_fs", &TThis::EventsOnFs)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
