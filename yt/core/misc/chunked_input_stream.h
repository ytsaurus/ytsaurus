#pragma once

#include "common.h"
#include "ref.h"

#include <util/stream/input.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TChunkedInputStream
    : public IZeroCopyInput
{
public:
    explicit TChunkedInputStream(const std::vector<TSharedRef>& blocks);

    virtual size_t DoNext(const void** ptr, size_t len) override;

private:
    void SkipCompletedBlocks();

    const std::vector<TSharedRef>& Blocks_;
    size_t Index_;
    size_t Position_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
