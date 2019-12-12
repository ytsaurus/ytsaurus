#include "log_rotator.h"

#include "bootstrap.h"
#include "log_reader.h"
#include "log_tailer.h"

#include <yt/core/misc/fs.h>

namespace NYT::NLogTailer {

using namespace NConcurrency;
using namespace NFS;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("LogRotator");

////////////////////////////////////////////////////////////////////////////////

TLogRotator::TLogRotator(const TLogRotationConfigPtr& config, TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Config_(config)
    , LogRotatorExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetRotatorInvoker(),
        BIND(&TLogRotator::RotateLogs, MakeWeak(this)),
        config->RotationPeriod))
{
    if (Config_->Enable && !Config_->LogWriterPid) {
        THROW_ERROR_EXCEPTION("Log rotation is enabled while writer pid is not set");
    }

    LogFilePaths_.reserve(Bootstrap_->GetConfig()->LogFiles.size());
    for (const auto& file : Bootstrap_->GetConfig()->LogFiles) {
        LogFilePaths_.emplace_back(file->Path);
    }
}

void TLogRotator::Start()
{
    if (Config_->Enable) {
        LogRotatorExecutor_->Start();
        YT_LOG_INFO("Log rotation started (RotationPeriod: %v)", Config_->RotationPeriod);
    }
}

void TLogRotator::Stop()
{
    if (Config_->Enable) {
        WaitFor(LogRotatorExecutor_->Stop())
           .ThrowOnError();
    }
}

void TLogRotator::RotateLogs()
{
    VERIFY_INVOKER_AFFINITY(Bootstrap_->GetRotatorInvoker());

    ++RotationCount_;
    YT_LOG_INFO("Rotating log (RotationCount: %v)", RotationCount_);
    if (RotationCount_ == 1) {
        YT_LOG_INFO("Ignoring first rotation");
        return;
    }

    for (const auto& file : LogFilePaths_) {
        int segmentCount = 0;
        while (Exists(GetLogSegmentPath(file, segmentCount))) {
            ++segmentCount;
        }

        YT_LOG_INFO("Moving log segments (LogName: %v, SegmentCount: %v)",
            file,
            segmentCount);

        if (segmentCount == Config_->LogSegmentCount) {
            auto lastLogSegmentPath = GetLogSegmentPath(file, segmentCount - 1);
            YT_LOG_INFO("Removing last log segment (FileName: %v)", lastLogSegmentPath);
            Remove(lastLogSegmentPath);
            --segmentCount;
        }

        for (int segmentId = segmentCount; segmentId >= 1; --segmentId) {
            auto oldLogSegmentPath = GetLogSegmentPath(file, segmentId - 1);
            auto newLogSegmentPath = GetLogSegmentPath(file, segmentId);

            YT_LOG_DEBUG("Renaming log segment (OldName: %v, NewName: %v)",
                oldLogSegmentPath,
                newLogSegmentPath);
            Rename(oldLogSegmentPath, newLogSegmentPath);
        }
    }

    auto logWriterPid = *Config_->LogWriterPid;

    YT_LOG_DEBUG("Sending SIGHUP to process (LogWriterPid: %v)", logWriterPid);
    int killResult = kill(logWriterPid, SIGHUP);
    if (killResult != 0 && LastSystemError() != ESRCH) {
        YT_LOG_ERROR("Unexpected kill result (LogWriterPid: %v, KillResult: %v)",
            logWriterPid,
            LastSystemErrorText());
    }

    Sleep(Config_->RotationDelay);

    for (const auto& reader : Bootstrap_->GetLogTailer()->GetLogReaders()) {
        reader->OnLogRotation();
    }
}

TString TLogRotator::GetLogSegmentPath(const TString& logFilePath, int segmentId)
{
    if (segmentId == 0) {
        return logFilePath;
    } else {
        return Format("%v.%v", logFilePath, segmentId);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
