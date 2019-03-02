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

void TMessageStringBuilder::DoReset()
{
    Buffer_.Reset();
}

void TMessageStringBuilder::DoPreallocate(size_t newLength)
{
    auto oldLength = GetLength();
    newLength = FastClp2(newLength);
    auto* context = GetContext();
    if (Y_UNLIKELY(context->ChunkOffset + newLength > context->Chunk.Size())) {
        auto chunkSize = std::max(ChunkSize, newLength);
        context->Chunk = TSharedMutableRef::Allocate<TMessageBufferTag>(chunkSize, false);
        context->ChunkOffset = 0;
    }
    auto oldBuffer = std::move(Buffer_); // Hold the old buffer for a while.
    Buffer_ = context->Chunk.Slice(context->ChunkOffset, context->ChunkOffset + newLength);
    context->ChunkOffset += newLength;
    ::memcpy(Buffer_.Begin(), Begin_, oldLength);
    Begin_ = Buffer_.Begin();
    End_ = Begin_ + newLength;
}

TMessageStringBuilder::TContext* TMessageStringBuilder::GetContext()
{
    if (Y_UNLIKELY(!Context_)) {
        Y_STATIC_THREAD(TContext) Context;
        Context_ = &Context;
    }
    return Context_;
}

Y_POD_THREAD(TMessageStringBuilder::TContext*) TMessageStringBuilder::Context_;

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
