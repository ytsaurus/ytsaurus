#pragma once

#include "fwd.h"
#include "raw_transform.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TRawFlatten
    : public IRawFlatten
{
public:
    TRawFlatten() = default;

    TRawFlatten(TRowVtable rowVtable, ssize_t inputCount)
        : RowVtable_(std::move(rowVtable))
        , InputCount_(inputCount)
    { }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        std::vector<TDynamicTypeTag> result;
        for (ssize_t i = 0; i < InputCount_; ++i) {
            result.emplace_back("flatten-input-" + ToString(i), RowVtable_);
        }
        return result;
    }

    virtual std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {TDynamicTypeTag{"flatten-output", RowVtable_}};
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawFlattenPtr {
            return ::MakeIntrusive<TRawFlatten>();
        };
    }

    void SaveState(IOutputStream& stream) const override
    {
        ::Save(&stream, InputCount_);
        ::Save(&stream, RowVtable_);
    }

    void LoadState(IInputStream& stream) override
    {
        ::Load(&stream, InputCount_);
        ::Load(&stream, RowVtable_);
    }

private:
    TRowVtable RowVtable_;
    ssize_t InputCount_ = 0;
};


template <typename TRow>
IRawFlattenPtr MakeRawFlatten(TRowVtable rowVtable, ssize_t inputCount)
{
    return new TRawFlatten(rowVtable, inputCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
