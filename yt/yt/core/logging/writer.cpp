#include "writer.h"
#include "private.h"
#include "log.h"
#include "random_access_gzip.h"
#include "appendable_zstd.h"

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

namespace NYT::NLogging {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const TLogger Logger(SystemLoggingCategoryName);
static constexpr size_t BufferSize = 1 << 16;

////////////////////////////////////////////////////////////////////////////////

TRateLimitCounter::TRateLimitCounter(
    std::optional<size_t> limit,
    NProfiling::TCounter bytesCounter,
    NProfiling::TCounter skippedEventsCounter)
    : LastUpdate_(TInstant::Now())
    , RateLimit_(limit)
    , BytesCounter_(std::move(bytesCounter))
    , SkippedEventsCounter_(std::move(skippedEventsCounter))
{ }

void TRateLimitCounter::SetRateLimit(std::optional<size_t> rateLimit)
{
    RateLimit_ = rateLimit;
    LastUpdate_ = TInstant::Now();
    BytesWritten_ = 0;
}

bool TRateLimitCounter::IsLimitReached()
{
    if (!RateLimit_) {
        return false;
    }

    if(BytesWritten_ >= *RateLimit_) {
        SkippedEvents_++;
        SkippedEventsCounter_.Increment();
        return true;
    } else {
        return false;
    }
}

bool TRateLimitCounter::IsIntervalPassed()
{
    auto now = TInstant::Now();
    if (now - LastUpdate_ >= UpdatePeriod_) {
        LastUpdate_ = now;
        BytesWritten_ = 0;
        return true;
    }
    return false;
}

void TRateLimitCounter::UpdateCounter(size_t bytesWritten)
{
    BytesWritten_ += bytesWritten;
    BytesCounter_.Increment(bytesWritten);
}

i64 TRateLimitCounter::GetAndResetLastSkippedEventsCount()
{
    i64 old = SkippedEvents_;
    SkippedEvents_ = 0;
    return old;
}

////////////////////////////////////////////////////////////////////////////////

TStreamLogWriterBase::TStreamLogWriterBase(std::unique_ptr<ILogFormatter> formatter, TString name)
    : LogFormatter_(std::move(formatter))
    , Name_(std::move(name))
    , RateLimit_(
        std::nullopt,
        {},
        TProfiler{"/logging"}.WithSparse().WithTag("writer", name).Counter("/events_skipped_by_global_limit"))
{ }

TStreamLogWriterBase::~TStreamLogWriterBase() = default;

void TStreamLogWriterBase::Write(const TLogEvent& event)
{
    auto* stream = GetOutputStream();
    if (!stream) {
        return;
    }

    try {
        auto* categoryRateLimit = GetCategoryRateLimitCounter(event.Category->Name);
        if (RateLimit_.IsIntervalPassed()) {
            auto eventsSkipped = RateLimit_.GetAndResetLastSkippedEventsCount();
            if (eventsSkipped > 0) {
                LogFormatter_->WriteLogSkippedEvent(stream, eventsSkipped, Name_);
            }
        }
        if (categoryRateLimit->IsIntervalPassed()) {
            auto eventsSkipped = categoryRateLimit->GetAndResetLastSkippedEventsCount();
            if (eventsSkipped > 0) {
                LogFormatter_->WriteLogSkippedEvent(stream, eventsSkipped, event.Category->Name);
            }
        }
        if (!RateLimit_.IsLimitReached() && !categoryRateLimit->IsLimitReached()) {
            auto bytesWritten = LogFormatter_->WriteFormatted(stream, event);
            RateLimit_.UpdateCounter(bytesWritten);
            categoryRateLimit->UpdateCounter(bytesWritten);
        }
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

void TStreamLogWriterBase::CheckSpace(i64 /*minSpace*/)
{ }

void TStreamLogWriterBase::OnException(const std::exception& ex)
{
    // Fail with drama by default.
    TRawFormatter<1024> formatter;
    formatter.AppendString("\n*** Unhandled exception in log writer: ");
    formatter.AppendString(ex.what());
    formatter.AppendString("\n*** Aborting ***\n");

    HandleEintr(::write, 2, formatter.GetData(), formatter.GetBytesWritten());

    _exit(100);
    YT_ABORT();
}

void TStreamLogWriterBase::SetRateLimit(std::optional<size_t> limit)
{
    RateLimit_.SetRateLimit(limit);
}

void TStreamLogWriterBase::SetCategoryRateLimits(const THashMap<TString, size_t>& categoryRateLimits)
{
    CategoryToRateLimit_.clear();
    for (const auto& it : categoryRateLimits) {
        GetCategoryRateLimitCounter(it.first)->SetRateLimit(it.second);
    }
}

TRateLimitCounter* TStreamLogWriterBase::GetCategoryRateLimitCounter(TStringBuf category)
{
    auto it = CategoryToRateLimit_.find(category);
    if (it == CategoryToRateLimit_.end()) {
        auto r = TProfiler{"/logging"}
            .WithSparse()
            .WithTag("writer", Name_)
            .WithTag("category", TString{category}, -1);

        // TODO(prime@): optimize sensor count
        it = CategoryToRateLimit_.insert({category, TRateLimitCounter(
            std::nullopt,
            r.Counter("/bytes_written"),
            r.Counter("/events_skipped_by_category_limit")
        )}).first;
    }
    return &it->second;
}

////////////////////////////////////////////////////////////////////////////////

TStreamLogWriter::TStreamLogWriter(IOutputStream* stream, std::unique_ptr<ILogFormatter> formatter, TString name)
    : TStreamLogWriterBase::TStreamLogWriterBase(std::move(formatter), std::move(name))
    , Stream_(stream)
{ }

IOutputStream* TStreamLogWriter::GetOutputStream() const noexcept
{
    return Stream_;
}

////////////////////////////////////////////////////////////////////////////////

TStderrLogWriter::TStderrLogWriter()
    : TStreamLogWriterBase::TStreamLogWriterBase(
        std::make_unique<TPlainTextLogFormatter>(),
        TString("stderr"))
{ }

IOutputStream* TStderrLogWriter::GetOutputStream() const noexcept
{
    return &Cerr;
}

////////////////////////////////////////////////////////////////////////////////

IOutputStream* TStdoutLogWriter::GetOutputStream() const noexcept
{
    return &Cout;
}

////////////////////////////////////////////////////////////////////////////////

TFileLogWriter::TFileLogWriter(
    std::unique_ptr<ILogFormatter> formatter,
    TString writerName,
    TString fileName,
    bool enableCompression,
    ECompressionMethod compressionMethod,
    int compressionLevel)
    : TStreamLogWriterBase(std::move(formatter), std::move(writerName))
    , FileName_(std::move(fileName))
    , EnableCompression_(enableCompression)
    , CompressionMethod_(compressionMethod)
    , CompressionLevel_(compressionLevel)
{
    Open();
}

TFileLogWriter::~TFileLogWriter() = default;

IOutputStream* TFileLogWriter::GetOutputStream() const noexcept
{
    if (Y_UNLIKELY(Disabled_.load(std::memory_order_acquire))) {
        return nullptr;
    }
    return OutputStream_.get();
}

void TFileLogWriter::OnException(const std::exception& ex)
{
    Disabled_ = true;
    YT_LOG_ERROR(ex, "Disabled log file (FileName: %v)", FileName_);

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
                YT_LOG_ERROR("Log file disabled: not enough space available (FileName: %v, AvailableSpace: %v, MinSpace: %v)",
                    directoryName,
                    statistics.AvailableSpace,
                    minSpace);

                Close();
            }
        } else {
            if (Disabled_.load(std::memory_order_acquire)) {
                Reload(); // Reinitialize all descriptors.

                YT_LOG_INFO("Log file enabled: space check passed (FileName: %v)", FileName_);
                Disabled_ = false;
            }
        }
    } catch (const std::exception& ex) {
        Disabled_ = true;
        YT_LOG_ERROR(ex, "Log file disabled: space check failed (FileName: %v)", FileName_);

        Close();
    }
}

void TFileLogWriter::Open()
{
    Disabled_ = false;
    try {
        NFS::MakeDirRecursive(NFS::GetDirectoryName(FileName_));

        TFlags<EOpenModeFlag> openMode;
        if (EnableCompression_) {
            openMode = OpenAlways|RdWr|CloseOnExec;
        } else {
            openMode = OpenAlways|ForAppend|WrOnly|Seq|CloseOnExec;
        }

        File_.reset(new TFile(FileName_, openMode));

        if (!EnableCompression_) {
            auto output = std::make_unique<TFixedBufferFileOutput>(*File_, BufferSize);
            output->SetFinishPropagateMode(true);
            OutputStream_ = std::move(output);
        } else if (CompressionMethod_ == ECompressionMethod::Zstd) {
            OutputStream_.reset(new TAppendableZstdFile(File_.get(), CompressionLevel_, true));
        } else {
            OutputStream_.reset(new TRandomAccessGZipFile(File_.get(), CompressionLevel_));
        }

        // Emit a delimiter for ease of navigation.
        if (File_->GetLength() > 0) {
            LogFormatter_->WriteLogReopenSeparator(GetOutputStream());
        }

        LogFormatter_->WriteLogStartEvent(GetOutputStream());
    } catch (const std::exception& ex) {
        Disabled_ = true;
        YT_LOG_ERROR(ex, "Failed to open log file (FileName: %v)", FileName_);

        Close();
    } catch (...) {
        YT_ABORT();
    }
}

void TFileLogWriter::Close()
{
    try {
        if (OutputStream_) {
            OutputStream_->Finish();
            OutputStream_.reset();
        }

        if (File_) {
            File_->Close();
            File_.reset();
        }
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to close log file; ignored (FileName: %v)", FileName_);
    } catch (...) {
        YT_ABORT();
    }
}

void TFileLogWriter::Reload()
{
    Close();
    Open();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
