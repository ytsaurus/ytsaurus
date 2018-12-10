#include "details.h"

namespace NYT::NCompression {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger CompressionLogger("Compression");

////////////////////////////////////////////////////////////////////////////////

TVectorRefsSource::TVectorRefsSource(const std::vector<TSharedRef>& blocks)
    : Blocks_(blocks)
    , Available_(GetByteSize(blocks))
    , Index_(0)
    , Position_(0)
{
    SkipCompletedBlocks();
}

size_t TVectorRefsSource::Available() const
{
    return Available_;
}

const char* TVectorRefsSource::Peek(size_t* len)
{
    if (Index_ == Blocks_.size()) {
        *len = 0;
        return nullptr;
    }
    *len = Blocks_[Index_].Size() - Position_;
    return Blocks_[Index_].Begin() + Position_;
}

void TVectorRefsSource::Skip(size_t n)
{
    while (n > 0 && Index_ < Blocks_.size()) {
        size_t toSkip = std::min(Blocks_[Index_].Size() - Position_, n);

        Position_ += toSkip;
        SkipCompletedBlocks();

        n -= toSkip;
        Available_ -= toSkip;
    }
}

void TVectorRefsSource::SkipCompletedBlocks()
{
    while (Index_ < Blocks_.size() && Position_ == Blocks_[Index_].Size()) {
        Index_ += 1;
        Position_ = 0;
    }
}

////////////////////////////////////////////////////////////////////////////////

TDynamicByteArraySink::TDynamicByteArraySink(TBlob* output)
    : Output_(output)
{ }

void TDynamicByteArraySink::Append(const char* data, size_t n)
{
    Output_->Append(data, n);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression
