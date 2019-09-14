#include "log.h"
#include "log_manager.h"

#include <yt/core/misc/serialize.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TSharedRef TMessageStringBuilder::Flush()
{
    return Buffer_.Slice(0, GetLength());
}

void TMessageStringBuilder::DisablePerThreadCache()
{
#ifndef __APPLE__
    Cache_ = nullptr;
    CacheDestroyed_ = true;
#endif
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
#ifndef __APPLE__
    if (Y_LIKELY(Cache_)) {
        return Cache_;
    }
    if (CacheDestroyed_) {
        return nullptr;
    }
    static thread_local TPerThreadCache Cache;
    Cache_ = &Cache;
    return Cache_;
#else
    return nullptr;
#endif
}

TMessageStringBuilder::TPerThreadCache::~TPerThreadCache()
{
    TMessageStringBuilder::DisablePerThreadCache();
}

#ifndef __APPLE__
thread_local TMessageStringBuilder::TPerThreadCache* TMessageStringBuilder::Cache_;
thread_local bool TMessageStringBuilder::CacheDestroyed_;
#endif

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TLogger::TLogger()
    : LogManager_(nullptr)
    , Category_(nullptr)
{ }

TLogger::TLogger(const char* categoryName)
    : LogManager_(TLogManager::Get())
    , Category_(LogManager_->GetCategory(categoryName))
{ }

TLogger::operator bool() const
{
    return LogManager_;
}

const TLoggingCategory* TLogger::GetCategory() const
{
    return Category_;
}

bool TLogger::IsLevelEnabled(ELogLevel level) const
{
    if (!Category_) {
        return false;
    }

    if (Category_->CurrentVersion != Category_->ActualVersion->load(std::memory_order_relaxed)) {
        LogManager_->UpdateCategory(const_cast<TLoggingCategory*>(Category_));
    }

    return level >= Category_->MinLevel;
}

bool TLogger::GetAbortOnAlert() const
{
    return LogManager_->GetAbortOnAlert();
}

bool TLogger::IsPositionUpToDate(const TLoggingPosition& position) const
{
    return !Category_ || position.CurrentVersion == Category_->ActualVersion->load(std::memory_order_relaxed);
}

void TLogger::UpdatePosition(TLoggingPosition* position, TStringBuf message) const
{
    LogManager_->UpdatePosition(position, message);
}

void TLogger::Write(TLogEvent&& event) const
{
    LogManager_->Enqueue(std::move(event));
}

TLogger& TLogger::AddRawTag(const TString& tag)
{
    if (!Context_.empty()) {
        Context_ += ", ";
    }
    Context_ += tag;
    return *this;
}

const TString& TLogger::GetContext() const
{
    return Context_;
}

void TLogger::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    Save(context, TString(Category_->Name));
    Save(context, Context_);
}

void TLogger::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    TString categoryName;
    Load(context, categoryName);
    LogManager_ = TLogManager::Get();
    Category_ = LogManager_->GetCategory(categoryName.data());
    Load(context, Context_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
