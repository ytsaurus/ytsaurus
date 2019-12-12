#include "log_tailer.h"

#include "log_reader.h"
#include "log_rotator.h"
#include "log_writer_liveness_checker.h"

#include <util/system/env.h>

namespace NYT::NLogTailer {

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("LogTailer");

////////////////////////////////////////////////////////////////////////////////

TLogTailer::TLogTailer(
    TBootstrap* bootstrap,
    TLogTailerConfigPtr config)
    : Bootstrap_(bootstrap)
    , Config_(std::move(config))
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

    for (auto& reader : LogReaders_) {
        reader->Start();
    }

    LogRotator_ = New<TLogRotator>(Config_->LogRotation, Bootstrap_);
    LogRotator_->Start();

    LogWriterLivenessChecker_ = New<TLogWriterLivenessChecker>(Config_->LogWriterLivenessChecker, Bootstrap_);
    LogWriterLivenessChecker_->Start();

    YT_LOG_INFO("Log tailer started");
}

const std::vector<TLogFileReaderPtr>& TLogTailer::GetLogReaders() const
{
    return LogReaders_;
}

const TLogRotatorPtr& TLogTailer::GetLogRotator() const
{
    return LogRotator_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogTailer
