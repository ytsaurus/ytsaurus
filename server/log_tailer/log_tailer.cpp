#include "log_tailer.h"

#include "bootstrap.h"
#include "log_reader.h"
#include "log_rotator.h"
#include "log_writer_liveness_checker.h"

#include <util/system/env.h>

namespace NYT::NLogTailer {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("LogTailer");

////////////////////////////////////////////////////////////////////////////////

TLogTailer::TLogTailer(
    TBootstrap* bootstrap,
    TLogTailerConfigPtr config)
    : Bootstrap_(bootstrap)
    , Config_(std::move(config))
    , LogTailerExecutor_(New<TPeriodicExecutor>(
        Bootstrap_->GetLogTailerInvoker(),
        BIND(&TLogTailer::OnTick, MakeStrong(this)),
        Bootstrap_->GetConfig()->TickPeriod))
{ }

void TLogTailer::Run()
{
    LogReaders_.reserve(Config_->LogFiles.size());

    std::vector<std::pair<TString, TString>> extraLogTableColumns = {
        {"job_id", GetEnv("YT_JOB_ID")},
        {"operation_id", GetEnv("YT_OPERATION_ID")}};

    for (const auto& file : Config_->LogFiles) {
        LogReaders_.emplace_back(New<TLogFileReader>(file, Bootstrap_, extraLogTableColumns));
    }

    LogRotator_ = New<TLogRotator>(Config_->LogRotation, Bootstrap_);

    LogWriterLivenessChecker_ = New<TLogWriterLivenessChecker>(Config_->LogWriterLivenessChecker, Bootstrap_);

    LogTailerExecutor_->Start();

    YT_LOG_INFO("Log tailer started");
}

const std::vector<TLogFileReaderPtr>& TLogTailer::GetLogReaders() const
{
    return LogReaders_;
}

void TLogTailer::OnTick()
{
    YT_LOG_DEBUG("Tick started");

    LogRotator_->RotateLogs();

    for (auto& reader : LogReaders_) {
        reader->ReadLog();
    }

    LogWriterLivenessChecker_->CheckLiveness();

    YT_LOG_DEBUG("Tick finished");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
