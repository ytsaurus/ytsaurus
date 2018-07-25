#include "writer.h"
#include "private.h"
#include "log.h"
#include "log_manager.h"

#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>

namespace NYT {
namespace NLogging {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const TLogger Logger(SystemLoggingCategoryName);
static constexpr size_t BufferSize = 1 << 16;


TStreamLogWriterBase::TStreamLogWriterBase(std::unique_ptr<ILogFormatter> formatter)
    : LogFormatter(std::move(formatter))
{ }

TStreamLogWriterBase::~TStreamLogWriterBase() = default;

void TStreamLogWriterBase::Write(const TLogEvent& event)
{
    auto* stream = GetOutputStream();
    if (!stream) {
        return;
    }
    try {
        LogFormatter->WriteFormatted(stream, event);
    } catch (const std::exception& ex) {
        OnException(ex);
    }
}

void TStreamLogWriterBase::Flush()
{
    auto* stream = GetOutputStream();
    if (!stream) {
        return;
    }

    try {
        stream->Flush();
    } catch (const std::exception& ex) {
        OnException(ex);
    }
}

void TStreamLogWriterBase::Reload()
{ }

void TStreamLogWriterBase::CheckSpace(i64 minSpace)
{ }

void TStreamLogWriterBase::OnException(const std::exception& ex)
{
    // Fail with drama by default.
    TRawFormatter<1024> formatter;
    formatter.AppendString("\n*** Unhandled exception in log writer: ");
    formatter.AppendString(ex.what());
    formatter.AppendString("\n*** Aborting ***\n");

    HandleEintr(::write, 2, formatter.GetData(), formatter.GetBytesWritten());

    std::terminate();
}

////////////////////////////////////////////////////////////////////////////////

IOutputStream* TStreamLogWriter::GetOutputStream() const noexcept
{
    return Stream_;
}

////////////////////////////////////////////////////////////////////////////////

IOutputStream* TStderrLogWriter::GetOutputStream() const noexcept
{
    return &Cerr;
}

TStderrLogWriter::TStderrLogWriter()
    : TStreamLogWriterBase::TStreamLogWriterBase(std::make_unique<TPlainTextLogFormatter>())
{ }

////////////////////////////////////////////////////////////////////////////////

IOutputStream* TStdoutLogWriter::GetOutputStream() const noexcept
{
    return &Cout;
}

////////////////////////////////////////////////////////////////////////////////

TFileLogWriter::TFileLogWriter(std::unique_ptr<ILogFormatter> formatter, TString fileName)
    : TStreamLogWriterBase(std::move(formatter))
    , FileName_(std::move(fileName))
{
    Open();
}

TFileLogWriter::~TFileLogWriter() = default;

IOutputStream* TFileLogWriter::GetOutputStream() const noexcept
{
    if (Y_LIKELY(!Disabled_.load(std::memory_order_acquire))) {
        return FileOutput_.get();
    } else {
        return nullptr;
    }
}

void TFileLogWriter::OnException(const std::exception& ex)
{
    Disabled_ = true;
    LOG_ERROR(ex, "Disabled log file (FileName: %v)", FileName_);

    Close();
}

void TFileLogWriter::CheckSpace(i64 minSpace)
{
    try {
        auto directoryName = NFS::GetDirectoryName(FileName_);
        auto statistics = NFS::GetDiskSpaceStatistics(directoryName);
        if (statistics.AvailableSpace < minSpace) {
            if (!Disabled_.load(std::memory_order_acquire)) {
                Disabled_ = true;
                LOG_ERROR("Log file disabled: not enough space available (FileName: %v, AvailableSpace: %v, MinSpace: %v)",
                    directoryName,
                    statistics.AvailableSpace,
                    minSpace);

                Close();
            }
        } else {
            if (Disabled_.load(std::memory_order_acquire)) {
                Reload(); // Reinitialize all descriptors.

                LOG_INFO("Log file enabled: space check passed (FileName: %v)", FileName_);
                Disabled_ = false;
            }
        }
    } catch (const std::exception& ex) {
        Disabled_ = true;
        LOG_ERROR(ex, "Log file disabled: space check failed (FileName: %v)", FileName_);

        Close();
    }
}

void TFileLogWriter::Open()
{
    try {
        NFS::MakeDirRecursive(NFS::GetDirectoryName(FileName_));
        File_.reset(new TFile(FileName_, OpenAlways|ForAppend|WrOnly|Seq|CloseOnExec));
        FileOutput_.reset(new TFixedBufferFileOutput(*File_, BufferSize));
        FileOutput_->SetFinishPropagateMode(true);

        // Emit a delimiter for ease of navigation.
        if (File_->GetLength() > 0) {
            *FileOutput_ << Endl;
        }

        LogFormatter->WriteLogStartEvent(GetOutputStream());
    } catch (const std::exception& ex) {
        Disabled_ = true;
        LOG_ERROR(ex, "Failed to open log file (FileName: %v)", FileName_);

        Close();
    } catch (...) {
        Y_UNREACHABLE();
    }
}

void TFileLogWriter::Close()
{
    try {
        if (FileOutput_) {
            FileOutput_->Flush();
            FileOutput_->Finish();
        }
        if (File_) {
            File_->Close();
        }
    } catch (const std::exception& ex) {
        Disabled_ = true;
        LOG_ERROR(ex, "Failed to close log file %v", FileName_);
    } catch (...) {
        Y_UNREACHABLE();
    }

    try {
        FileOutput_.reset();
        File_.reset();
    } catch (...) {
        Y_UNREACHABLE();
    }
}

void TFileLogWriter::Reload()
{
    Close();
    Open();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT
