#include "config.h"

#include <yt/yt/server/lib/user_job_synchronizer_client/user_job_synchronizer.h>

namespace NYT::NUserJobExecutor {

////////////////////////////////////////////////////////////////////////////////

TUserJobExecutorConfig::TUserJobExecutorConfig()
{
    RegisterParameter("command", Command);

    RegisterParameter("pipes", Pipes)
        .Default();

    RegisterParameter("job_id", JobId);

    RegisterParameter("environment", Environment)
        .Default();

    RegisterParameter("uid", Uid)
        .Default(-1);

    RegisterParameter("enable_core_dump", EnableCoreDump)
        .Default(false);

    RegisterParameter("user_job_synchronizer_connection_config", UserJobSynchronizerConnectionConfig);

    RegisterPostprocessor([&] {
        for (const auto& variable : Environment) {
            if (variable.find('=') == TString::npos) {
                THROW_ERROR_EXCEPTION("Bad environment variable: missing '=' in %Qv", variable);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TUserJobExecutorConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NUserJobExector
