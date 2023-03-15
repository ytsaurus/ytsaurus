#pragma once

#include "raw_transform.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TRawMultiWrite
    : public IRawWrite
{
public:
    TRawMultiWrite();
    TRawMultiWrite(std::vector<IRawWritePtr> writes);

    void AddRaw(const void* row, ssize_t count) override;
    void Close() override;

    std::vector<TDynamicTypeTag> GetInputTags() const override;
    std::vector<TDynamicTypeTag> GetOutputTags() const override;

    TDefaultFactoryFunc GetDefaultFactory() const override;
    void SaveState(IOutputStream& stream) const override;
    void LoadState(IInputStream& stream) override;

private:
    std::vector<IRawWritePtr> Writes_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
