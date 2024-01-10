#include "raw_multi_write.h"

#include "../type_tag.h"

#include <vector>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

TRawMultiWrite::TRawMultiWrite() = default;

TRawMultiWrite::TRawMultiWrite(std::vector<IRawWritePtr> writes)
    : Writes_(std::move(writes))
{
    Y_ABORT_UNLESS(!Writes_.empty(), "Expected non empty transform list");

    auto getRowVtable = [&] (size_t idx) {
        auto tags = writes[idx]->GetOutputTags();
        Y_ABORT_UNLESS(tags.size() == 1, "Raw write output tag size: %d (expected: 1)", static_cast<int>(tags.size()));
        return tags.front().GetRowVtable();
    };

    auto rowVtable = getRowVtable(0);
    for (int i = 0; i < std::ssize(Writes_); ++i) {
        auto currentVtable = getRowVtable(1);
        if (currentVtable.TypeName != rowVtable.TypeName) {
            Y_ABORT("Types for #0 and #%d transforms differ", i);
        }
    }
}

void TRawMultiWrite::AddRaw(const void* row, ssize_t count)
{
    for (const auto& write : Writes_) {
        write->AddRaw(row, count);
    }
}

void TRawMultiWrite::Close()
{
    for (const auto& write : Writes_) {
        write->Close();
    }
}

std::vector<TDynamicTypeTag> TRawMultiWrite::GetInputTags() const
{
    return {
        {}
    };
}

std::vector<TDynamicTypeTag> TRawMultiWrite::GetOutputTags() const
{
    return {};
}

ISerializable<IRawWrite>::TDefaultFactoryFunc TRawMultiWrite::GetDefaultFactory() const
{
    return []() -> IRawWritePtr {
        return ::MakeIntrusive<TRawMultiWrite>();
    };
}

void TRawMultiWrite::Save(IOutputStream* stream) const
{
    ::Save(stream, static_cast<i64>(Writes_.size()));

    for (const auto& write : Writes_) {
        SaveSerializable(stream, write);
    }
}

void TRawMultiWrite::Load(IInputStream* stream)
{
    i64 writeCount;
    ::Load(stream, writeCount);

    for (i64 i = 0; i < writeCount; ++i) {
        IRawWritePtr write;
        LoadSerializable(stream, write);
        Writes_.push_back(write);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
