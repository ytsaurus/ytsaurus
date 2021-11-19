#include "logger.h"

#include <library/cpp/yt/assert/assert.h>

#include <util/system/compiler.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TSharedRef TMessageStringBuilder::Flush()
{
    return Buffer_.Slice(0, GetLength());
}

void TMessageStringBuilder::DisablePerThreadCache()
{
    Cache_ = nullptr;
    CacheDestroyed_ = true;
}

void TMessageStringBuilder::DoReset()
{
    Buffer_.Reset();
}

void TMessageStringBuilder::DoPreallocate(size_t newLength)
{
    auto oldLength = GetLength();
    newLength = FastClp2(newLength);
    auto newChunkSize = std::max(ChunkSize, newLength);
    // Hold the old buffer until the data is copied.
    auto oldBuffer = std::move(Buffer_);
    auto* cache = GetCache();
    if (Y_LIKELY(cache)) {
        if (Y_UNLIKELY(cache->ChunkOffset + newLength > cache->Chunk.Size())) {
            cache->Chunk = TSharedMutableRef::Allocate<TMessageBufferTag>(newChunkSize, false);
            cache->ChunkOffset = 0;
        }
        Buffer_ = cache->Chunk.Slice(cache->ChunkOffset, cache->ChunkOffset + newLength);
        cache->ChunkOffset += newLength;
    } else {
        Buffer_ = TSharedMutableRef::Allocate<TMessageBufferTag>(newChunkSize, false);
        newLength = newChunkSize;
    }
    if (oldLength > 0) {
        ::memcpy(Buffer_.Begin(), Begin_, oldLength);
    }
    Begin_ = Buffer_.Begin();
    End_ = Begin_ + newLength;
}

TMessageStringBuilder::TPerThreadCache* TMessageStringBuilder::GetCache()
{
    if (Y_LIKELY(Cache_)) {
        return Cache_;
    }
    if (CacheDestroyed_) {
        return nullptr;
    }
    static thread_local TPerThreadCache Cache;
    Cache_ = &Cache;
    return Cache_;
}

TMessageStringBuilder::TPerThreadCache::~TPerThreadCache()
{
    TMessageStringBuilder::DisablePerThreadCache();
}

thread_local TMessageStringBuilder::TPerThreadCache* TMessageStringBuilder::Cache_;
thread_local bool TMessageStringBuilder::CacheDestroyed_;

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

Y_WEAK TLoggingContext GetLoggingContext()
{
    return {};
}

Y_WEAK ILogManager* GetDefaultLogManager()
{
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

TLogger::TLogger(ILogManager* logManager, TStringBuf categoryName)
    : LogManager_(logManager)
    , Category_(LogManager_ ? LogManager_->GetCategory(categoryName) : nullptr)
    , MinLevel_(LoggerDefaultMinLevel)
{ }

TLogger::TLogger(TStringBuf categoryName)
    : TLogger(GetDefaultLogManager(), categoryName)
{ }

TLogger::operator bool() const
{
    return LogManager_;
}

const TLoggingCategory* TLogger::GetCategory() const
{
    return Category_;
}

bool TLogger::IsLevelEnabledHeavy(ELogLevel level) const
{
    // Note that we managed to reach this point, i.e. level >= MinLevel_,
    // which implies that MinLevel_ != ELogLevel::Maximum, so this logger was not
    // default constructed, thus it has non-trivial category.
    YT_ASSERT(Category_);

    if (Category_->CurrentVersion != Category_->ActualVersion->load(std::memory_order_relaxed)) {
        LogManager_->UpdateCategory(const_cast<TLoggingCategory*>(Category_));
    }

    return level >= Category_->MinPlainTextLevel;
}

bool TLogger::GetAbortOnAlert() const
{
    return LogManager_->GetAbortOnAlert();
}

bool TLogger::IsEssential() const
{
    return Essential_;
}

void TLogger::UpdateAnchor(TLoggingAnchor* anchor) const
{
    LogManager_->UpdateAnchor(anchor);
}

void TLogger::RegisterStaticAnchor(TLoggingAnchor* anchor, ::TSourceLocation sourceLocation, TStringBuf message) const
{
    LogManager_->RegisterStaticAnchor(anchor, sourceLocation, message);
}

void TLogger::Write(TLogEvent&& event) const
{
    LogManager_->Enqueue(std::move(event));
}

void TLogger::AddRawTag(const TString& tag)
{
    if (!Tag_.empty()) {
        Tag_ += ", ";
    }
    Tag_ += tag;
}

TLogger TLogger::WithRawTag(const TString& tag) const
{
    auto result = *this;
    result.AddRawTag(tag);
    return result;
}

TLogger TLogger::WithEssential(bool essential) const
{
    auto result = *this;
    result.Essential_ = essential;
    return result;
}

TLogger TLogger::WithMinLevel(ELogLevel minLevel) const
{
    auto result = *this;
    result.MinLevel_ = minLevel;
    return result;
}

const TString& TLogger::GetTag() const
{
    return Tag_;
}

const TLogger::TStructuredTags& TLogger::GetStructuredTags() const
{
    return StructuredTags_;
}

////////////////////////////////////////////////////////////////////////////////

void LogStructuredEvent(
    const TLogger& logger,
    NYson::TYsonString message,
    ELogLevel level)
{
    YT_VERIFY(message.GetType() == NYson::EYsonType::MapFragment);
    auto loggingContext = GetLoggingContext();
    auto event = NDetail::CreateLogEvent(
        loggingContext,
        logger,
        level);
    event.StructuredMessage = std::move(message);
    event.Family = ELogFamily::Structured;
    logger.Write(std::move(event));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
