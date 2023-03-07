#include "writer.h"
#include "private.h"
#include "log.h"
#include "log_manager.h"
#include "random_access_gzip.h"

#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYT::NLogging {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const TLogger Logger(SystemLoggingCategoryName);
static constexpr size_t BufferSize = 1 << 16;

////////////////////////////////////////////////////////////////////////////////

TRateLimitCounter::TRateLimitCounter(
    std::optional<size_t> limit,
    const TMonotonicCounter& bytesCounter,
    const TMonotonicCounter& skippedEventsCounter)
    : LastUpdate_(TInstant::Now())
    , RateLimit_(limit)
    , BytesCounter_(bytesCounter)
    , SkippedEventsCounter_(skippedEventsCounter)
{ }

void TRateLimitCounter::SetRateLimit(std::optional<size_t> rateLimit)
{
    RateLimit_ = rateLimit;
    LastUpdate_ = TInstant::Now();
    BytesWritten_ = 0;
}

void TRateLimitCounter::SetBytesCounter(const TMonotonicCounter& counter)
{
    BytesCounter_ = counter;
}

void TRateLimitCounter::SetSkippedEventsCounter(const TMonotonicCounter& counter)
{
    SkippedEventsCounter_ = counter;
}

bool TRateLimitCounter::IsLimitReached()
{
    if (!RateLimit_) {
        return false;
    }

    if(BytesWritten_ >= *RateLimit_) {
        LoggingProfiler.Increment(SkippedEventsCounter_, 1);
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
    LoggingProfiler.Increment(BytesCounter_, bytesWritten);
}

i64 TRateLimitCounter::GetAndResetLastSkippedEventsCount()
{
    i64 old = SkippedEvents_;
    SkippedEvents_ = SkippedEventsCounter_.GetCurrent();
    return SkippedEvents_ - old;
}

////////////////////////////////////////////////////////////////////////////////

TStreamLogWriterBase::TStreamLogWriterBase(std::unique_ptr<ILogFormatter> formatter, TString name)
    : LogFormatter(std::move(formatter))
    , Name_(std::move(name))
    , RateLimit_(
        std::nullopt,
        TMonotonicCounter(),
        TMonotonicCounter("/log_events_skipped", {TProfileManager::Get()->RegisterTag("skipped_by", Name_)})
    )
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
                LogFormatter->WriteLogSkippedEvent(stream, eventsSkipped, Name_);
            }
        }
        if (categoryRateLimit->IsIntervalPassed()) {
            auto eventsSkipped = categoryRateLimit->GetAndResetLastSkippedEventsCount();
            if (eventsSkipped > 0) {
                LogFormatter->WriteLogSkippedEvent(stream, eventsSkipped, event.Category->Name);
            }
        }
        if (!RateLimit_.IsLimitReached() && !categoryRateLimit->IsLimitReached()) {
            size_t bytesWritten = LogFormatter->WriteFormatted(stream, event);
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

TRateLimitCounter* TStreamLogWriterBase::GetCategoryRateLimitCounter(const TString& category)
{
    auto it = CategoryToRateLimit_.find(category);
    if (it == CategoryToRateLimit_.end()) {
        auto tagId = TProfileManager::Get()->RegisterTag("writer_and_category", Name_ + "/" + category);
        auto skippedTagId = TProfileManager::Get()->RegisterTag("skipped_by", Name_ + "/" + category);
        TMonotonicCounter bytesCounter("/bytes_written", {tagId});
        TMonotonicCounter skippedEventsCounter("/log_events_skipped", {skippedTagId});
        it = CategoryToRateLimit_.insert({category, TRateLimitCounter(std::nullopt, bytesCounter, skippedEventsCounter)}).first;
    }
    return &it->second;
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
    : TStreamLogWriterBase::TStreamLogWriterBase(std::make_unique<TPlainTextLogFormatter>(), TString("stderr"))
{ }

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
    bool enableCompression)
    : TStreamLogWriterBase(std::move(formatter), std::move(writerName))
    , FileName_(std::move(fileName))
    , EnableCompression_(enableCompression)
{
    Open();
}

TFileLogWriter::~TFileLogWriter() = default;

IOutputStream* TFileLogWriter::GetOutputStream() const noexcept
{
    if (Y_UNLIKELY(Disabled_.load(std::memory_order_acquire))) {
        return nullptr;
    }
    if (CompressedOutput_) {
        return CompressedOutput_.get();
    }
    return FileOutput_.get();
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

        if (EnableCompression_) {
            CompressedOutput_.reset(new TRandomAccessGZipFile(File_.get()));
        } else {
            FileOutput_.reset(new TFixedBufferFileOutput(*File_, BufferSize));
            FileOutput_->SetFinishPropagateMode(true);
        }

        // Emit a delimiter for ease of navigation.
        if (File_->GetLength() > 0) {
            LogFormatter->WriteLogReopenSeparator(GetOutputStream());
        }

        LogFormatter->WriteLogStartEvent(GetOutputStream());
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
        if (CompressedOutput_) {
            CompressedOutput_->Finish();
            CompressedOutput_.reset();
        }

        if (FileOutput_) {
            FileOutput_->Flush();
            FileOutput_->Finish();
            FileOutput_.reset();
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
