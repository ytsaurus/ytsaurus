#include "log_writer_liveness_checker.h"

#include "bootstrap.h"
#include "config.h"
#include "log_reader.h"
#include "log_tailer.h"

#include <yt/core/logging/log_manager.h>

namespace NYT::NLogTailer {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("LogWriterLivenessChecker");

////////////////////////////////////////////////////////////////////////////////

TLogWriterLivenessChecker::TLogWriterLivenessChecker(
    const TLogWriterLivenessCheckerConfigPtr& config,
    TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Config_(config)
    , LogWriterLivenessCheckerExeuctor_(New<TPeriodicExecutor>(
        Bootstrap_->GetLogWriterLivenessCheckerInvoker(),
        BIND(&TLogWriterLivenessChecker::DoCheckLiveness, MakeWeak(this)),
        config->LivenessCheckPeriod))
{ }

void TLogWriterLivenessChecker::Start()
{
    if (Config_->Enable) {
        LogWriterLivenessCheckerExeuctor_->Start();
        YT_LOG_INFO("Log writer liveness checker started (LivenessCheckPeriod: %v)", Config_->LivenessCheckPeriod);
    }
}

void TLogWriterLivenessChecker::Stop()
{
    if (Config_->Enable) {
        WaitFor(LogWriterLivenessCheckerExeuctor_->Stop())
            .ThrowOnError();
    }
}

void TLogWriterLivenessChecker::DoCheckLiveness()
{
    auto logWriterPid = *Bootstrap_->GetConfig()->LogRotation->LogWriterPid;
    YT_LOG_INFO("Checking log writer liveness (LogWriterPid: %v)", logWriterPid);

    int killResult = kill(logWriterPid, 0);
    if (killResult == 0) {
        YT_LOG_INFO("Log writer is alive (LogWriterPid: %v)", logWriterPid);
    } else if (LastSystemError() == ESRCH) {
        YT_LOG_INFO("Log writer is dead; uploading rest of the log (LogWriterPid: %v)", logWriterPid);
        Bootstrap_->GetLogTailer()->GetLogRotator()->Stop();
        YT_LOG_INFO("Log rotator stopped working");
        for (const auto& reader : Bootstrap_->GetLogTailer()->GetLogReaders()) {
            reader->OnTermination();
        }

        YT_LOG_INFO("Log writer has stopped; terminating (LogWriterPid: %v)", logWriterPid);
        NLogging::TLogManager::Get()->Shutdown();
        Bootstrap_->Terminate();
    } else {
        YT_LOG_ERROR("Unexpected kill result (LogWriterPid: %v, KillResult: %v)",
            logWriterPid,
            LastSystemErrorText());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
