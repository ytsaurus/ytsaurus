#include "stdafx.h"
#include "writer.h"
#include "log.h"
#include "private.h"

#include <core/misc/fs.h>

#include <core/build.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

static TLogger Logger(SystemLoggingCategory);

static const size_t BufferSize = 1 << 16;

////////////////////////////////////////////////////////////////////////////////

namespace {

TLogEvent GetBannerEvent()
{
    return TLogEvent(
        SystemLoggingCategory,
        ELogLevel::Info,
        Format("Logging started (Version: %v, BuildHost: %v, BuildTime: %v)",
            GetVersion(),
            GetBuildHost(),
            GetBuildTime()));
}

} // namespace

TStreamLogWriter::TStreamLogWriter(TOutputStream* stream)
    : Stream_(stream)
    , Buffer_(new TMessageBuffer())
{ }

void TStreamLogWriter::Write(const TLogEvent& event)
{
    if (!Stream_) {
        return;
    }

    auto* buffer = Buffer_.get();
    buffer->Reset();

    FormatDateTime(buffer, event.DateTime);
    buffer->AppendChar('\t');
    FormatLevel(buffer, event.Level);
    buffer->AppendChar('\t');
    buffer->AppendString(~event.Category);
    buffer->AppendChar('\t');
    FormatMessage(buffer, event.Message);
    buffer->AppendChar('\t');
    if (event.ThreadId != NConcurrency::InvalidThreadId) {
        buffer->AppendNumber(event.ThreadId, 16);
    }
    buffer->AppendChar('\t');
    if (event.FiberId != NConcurrency::InvalidFiberId) {
        buffer->AppendNumber(event.FiberId, 16);
    }
    buffer->AppendChar('\t');
    if (event.TraceId != NTracing::InvalidTraceId) {
        buffer->AppendNumber(event.TraceId, 16);
    }
    buffer->AppendChar('\n');

    Stream_->Write(buffer->GetData(), buffer->GetBytesWritten());
}

void TStreamLogWriter::Flush()
{
    Stream_->Flush();
}

void TStreamLogWriter::Reload()
{ }

void TStreamLogWriter::CheckSpace(i64 minSpace)
{ }

////////////////////////////////////////////////////////////////////////////////

TStderrLogWriter::TStderrLogWriter()
    : TStreamLogWriter(&StdErrStream())
{ }

////////////////////////////////////////////////////////////////////////////////

TStdoutLogWriter::TStdoutLogWriter()
    : TStreamLogWriter(&StdOutStream())
{ }

////////////////////////////////////////////////////////////////////////////////

TFileLogWriter::TFileLogWriter(const Stroka& fileName)
    : FileName_(fileName)
    , Initialized_(false)
{
    Disabled_.store(false);
    EnsureInitialized();
}

void TFileLogWriter::CheckSpace(i64 minSpace)
{
    try {
        auto directoryName = NFS::GetDirectoryName(FileName_);
        auto statistics = NFS::GetDiskSpaceStatistics(directoryName);
        if (statistics.AvailableSpace < minSpace) {
            if (!Disabled_.load()) {
                LOG_ERROR("Logging disabled: not enough space (FileName: %v, AvailableSpace: %v, MinSpace: %v)",
                    directoryName,
                    statistics.AvailableSpace,
                    minSpace);
            }
            Disabled_.store(true);
        } else {
            if (Disabled_.load()) {
                LOG_INFO("Logging enabled: space check passed");
            }
            Disabled_.store(false);
        }
    } catch (const std::exception& ex) {
        Disabled_.store(true);
        LOG_ERROR(ex, "Logging disabled: space check failed (FileName: %v)",
            FileName_);
    }
}

void TFileLogWriter::ReopenFile()
{
    NFS::ForcePath(NFS::GetDirectoryName(FileName_));
    File_.reset(new TFile(FileName_, OpenAlways|ForAppend|WrOnly|Seq|CloseOnExec));
    FileOutput_.reset(new TBufferedFileOutput(*File_, BufferSize));
    FileOutput_->SetFinishPropagateMode(true);
}

void TFileLogWriter::EnsureInitialized(bool writeTrailingNewline)
{
    if (Initialized_) {
        return;
    }

    // No matter what, let's pretend we're initialized to avoid subsequent attempts.
    Initialized_ = true;

    if (Disabled_.load()) {
        return;
    }

    try {
        ReopenFile();
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error opening log file %v", FileName_);
        return;
    }

    Stream_ = FileOutput_.get();

    if (writeTrailingNewline) {
        *FileOutput_ << Endl;
    }
    Write(GetBannerEvent());
}

void TFileLogWriter::Write(const TLogEvent& event)
{
    if (Stream_ && !Disabled_.load()) {
        TStreamLogWriter::Write(event);
    }
}

void TFileLogWriter::Flush()
{
    if (Stream_ && !Disabled_.load()) {
        try {
            FileOutput_->Flush();
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Failed to flush log file %v", FileName_);
        }
    }
}

void TFileLogWriter::Reload()
{
    Flush();

    if (File_) {
        try {
            File_->Close();
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Failed to close log file %v", FileName_);
        }
    }

    Initialized_ = false;
    EnsureInitialized(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
