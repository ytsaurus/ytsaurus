#include "log.h"
#include "log_manager.h"

#include <yt/yt/core/misc/serialize.h>

#include <util/system/thread.h>

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

thread_local bool CachedThreadNameInitialized;
thread_local TLogEvent::TThreadName CachedThreadName;
thread_local int CachedThreadNameLength;

void CacheThreadName()
{
    if (auto name = TThread::CurrentThreadName()) {
        CachedThreadNameLength = std::min(TLogEvent::ThreadNameBufferSize - 1, static_cast<int>(name.length()));
        ::memcpy(CachedThreadName.data(), name.data(), CachedThreadNameLength);
    }
    CachedThreadNameInitialized = true;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TLogger::TLogger(TStringBuf categoryName)
    : LogManager_(TLogManager::Get())
    , Category_(LogManager_->GetCategory(categoryName))
    , MinLevel_(LoggerDefaultMinLevel)
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

void TLogger::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    if (Category_) {
        Save(context, true);
        Save(context, TString(Category_->Name));
    } else {
        Save(context, false);
    }

    Save(context, Essential_);
    Save(context, MinLevel_);
    Save(context, Tag_);
}

void TLogger::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    TString categoryName;

    bool categoryPresent;
    Load(context, categoryPresent);
    if (categoryPresent) {
        Load(context, categoryName);
        LogManager_ = TLogManager::Get();
        Category_ = LogManager_->GetCategory(categoryName.data());
    } else {
        Category_ = nullptr;
    }

    Load(context, Essential_);
    Load(context, MinLevel_);
    Load(context, Tag_);
}

////////////////////////////////////////////////////////////////////////////////

void TLoggerOwner::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    Save(context, Logger);
}

void TLoggerOwner::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    Load(context, Logger);
}

void TLoggerOwner::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
