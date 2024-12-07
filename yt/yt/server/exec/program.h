#pragma once

#include "public.h"

#include <yt/yt/server/lib/user_job/config.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>

namespace NYT::NExec {

////////////////////////////////////////////////////////////////////////////////

class TExecProgram
    : public virtual TProgram
    , public TProgramConfigMixin<NUserJob::TUserJobExecutorConfig>
{
public:
    TExecProgram();

protected:
    void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override;
    void OnError(const TString& message) noexcept override;

private:
    TFile ExecutorStderr_;
    TString JobId_;

    void LogToStderr(const TString& message);
    void OpenExecutorStderr();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExec
