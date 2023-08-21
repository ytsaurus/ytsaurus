#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/memory/ref.h>

#include <vector>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

/*!
 *
 * Empty block represents a special 'null value' and is often used in cases when
 * no data is available or to signal the end of stream.
 */
struct TBlock
{
    TBlock() = default;
    explicit TBlock(TSharedRef block);
    TBlock(TSharedRef block, TChecksum checksum);

    TSharedRef Data;
    TChecksum Checksum = NullChecksum;
    //! For columnar table chunks, index of the group. This field is used
    //! for block reordering (see TBlockReorderer).
    std::optional<int> GroupIndex;

    explicit operator bool() const;

    size_t Size() const;

    TError ValidateChecksum() const;

    TChecksum GetOrComputeChecksum() const;

    static std::vector<TBlock> Wrap(const std::vector<TSharedRef>& blocks);
    static std::vector<TBlock> Wrap(std::vector<TSharedRef>&& blocks);
    static std::vector<TSharedRef> Unwrap(const std::vector<TBlock>& blocks);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

#define BLOCK_INL_H_
#include "block-inl.h"
#undef BLOCK_INL_H_
