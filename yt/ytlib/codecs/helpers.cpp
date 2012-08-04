#include "stdafx.h"
#include "helpers.h"

#include <ytlib/misc/foreach.h>

namespace NYT {
namespace NCodec {

////////////////////////////////////////////////////////////////////////////////

size_t TotalLength(const std::vector<TSharedRef>& refs)
{
    size_t size = 0;
    FOREACH (const auto& ref, refs) {
        size += ref.Size();
    }
    return size;
}

TSharedRef MergeRefs(const std::vector<TSharedRef>& blocks)
{
    TBlob result(TotalLength(blocks));
    size_t pos = 0;
    FOREACH(const auto& block, blocks) {
        std::copy(block.Begin(), block.End(), result.begin() + pos);
        pos += block.Size();
    }
    return TSharedRef(MoveRV(result));
}

TSharedRef Apply(TConverter converter, const TSharedRef& ref)
{
    ByteArraySource source(ref.Begin(), ref.Size());
    std::vector<char> output;
    converter.Run(&source, &output);
    return TSharedRef(MoveRV(output));
}

TSharedRef Apply(TConverter converter, const std::vector<TSharedRef>& refs)
{
    if (refs.size() == 1) {
        return Apply(converter, refs.front());
    }
    VectorRefsSource source(refs);
    std::vector<char> output;
    converter.Run(&source, &output);
    return TSharedRef(MoveRV(output));
}

////////////////////////////////////////////////////////////////////////////////

VectorRefsSource::VectorRefsSource(const std::vector<TSharedRef>& blocks)
    : Blocks_(blocks)
    , Available_(TotalLength(blocks))
    , Index_(0)
    , Position_(0)
{
    SkipCompletedBlocks();
}

size_t VectorRefsSource::Available() const
{
    return Available_;
}

const char* VectorRefsSource::Peek(size_t* len)
{
    if (Index_ == Blocks_.size()) {
        *len = 0;
        return NULL;
    }
    *len = Blocks_[Index_].Size() - Position_;
    return Blocks_[Index_].Begin() + Position_;
}

void VectorRefsSource::Skip(size_t n)
{
    while (n > 0 && Index_ < Blocks_.size()) {
        size_t toSkip = std::min(Blocks_[Index_].Size() - Position_, n);

        Position_ += toSkip;
        SkipCompletedBlocks();

        n -= toSkip;
        Available_ -= toSkip;
    }
}

void VectorRefsSource::SkipCompletedBlocks()
{
    while (Index_ < Blocks_.size() && Position_ == Blocks_[Index_].Size()) {
        Index_ += 1;
        Position_ = 0;
    }
}

////////////////////////////////////////////////////////////////////////////////

DynamicByteArraySink::DynamicByteArraySink(std::vector<char>* output)
    : Output_(output)
{ }

void DynamicByteArraySink::Append(const char* data, size_t n)
{
    size_t newSize = Output_->size() + n;
    if (newSize > Output_->capacity()) {
        Output_->reserve(std::max(Output_->capacity() * 2, newSize));
    }

    auto outputPointer = Output_->data() + Output_->size();
    Output_->resize(newSize);
    std::copy(data, data + n, outputPointer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCodec
} // namespace NYT
