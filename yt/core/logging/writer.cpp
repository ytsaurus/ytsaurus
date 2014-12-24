#include "stdafx.h"
#include "writer.h"
#include "log.h"
#include "private.h"

#include <core/misc/fs.h>

#include <yt/build.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

static TLogger Logger(SystemLoggingCategory);

////////////////////////////////////////////////////////////////////////////////

namespace {

TLogEvent GetBannerEvent()
{
    return TLogEvent(
        SystemLoggingCategory,
        ELogLevel::Info,
        Sprintf("Logging started (Version: %s, BuildHost: %s, BuildTime: %s)",
            YT_VERSION,
            YT_BUILD_HOST,
            YT_BUILD_TIME));
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

    auto* buffer = ~Buffer_;
    buffer->Reset();

    FormatDateTime(buffer, event.DateTime);
    buffer->AppendChar('\t');
    FormatLevel(~Buffer_, event.Level);
    buffer->AppendChar('\t');
    buffer->AppendString(~event.Category);
    buffer->AppendChar('\t');
    FormatMessage(buffer, event.Message);
    buffer->AppendChar('\t');
    if (event.ThreadId != 0) {
        buffer->AppendNumber(event.ThreadId, 16);
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
    AtomicSet(NotEnoughSpace_, false);
    EnsureInitialized();
}

void TFileLogWriter::CheckSpace(i64 minSpace)
{
    try {
        auto directoryName = NFS::GetDirectoryName(FileName_);
        auto statistics = NFS::GetDiskSpaceStatistics(directoryName);
        if (statistics.AvailableSpace < minSpace) {
            AtomicSet(NotEnoughSpace_, true);
            LOG_ERROR("Logging disabled: not enough space (FileName: %s, AvailableSpace: %" PRId64 ", MinSpace: %" PRId64 ")",
                ~directoryName,
                statistics.AvailableSpace,
                minSpace);
        }
    } catch (const std::exception& ex) {
        AtomicSet(NotEnoughSpace_, true);
        LOG_ERROR(
            ex,
            "Disable log writer: space check failed (FileName: %s)",
            ~FileName_);
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

    if (NotEnoughSpace_) {
        return;
    }

    try {
        ReopenFile();
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Error opening log file %s", ~FileName_.Quote());
        return;
    }

    Stream_ = ~FileOutput_;

    if (writeTrailingNewline) {
        *FileOutput_ << Endl;
    }
    Write(GetBannerEvent());
}

void TFileLogWriter::Write(const TLogEvent& event)
{
    if (Stream_ && !NotEnoughSpace_) {
        TStreamLogWriter::Write(event);
    }
}

void TFileLogWriter::Flush()
{
    if (Stream_ && !NotEnoughSpace_) {
        try {
            FileOutput_->Flush();
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Failed to flush log file %s", ~FileName_.Quote());
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
            LOG_ERROR(ex, "Failed to close log file %s", ~FileName_.Quote());
        }
    }

    Initialized_ = false;
    EnsureInitialized(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
