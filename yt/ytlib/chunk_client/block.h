#pragma once

#include "public.h"

#include <yt/core/misc/public.h>
#include <yt/core/misc/ref.h>
#include <yt/core/misc/property.h>

#include <vector>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

/*!
 * Block == data + optional checksum.
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

    operator bool() const;

    size_t Size() const;

    bool IsChecksumValid() const;

    void ValidateChecksum() const;

    TChecksum GetOrComputeChecksum() const;

    static std::vector<TBlock> Wrap(const std::vector<TSharedRef>& blocks);
    static std::vector<TBlock> Wrap(std::vector<TSharedRef>&& blocks);
    static std::vector<TSharedRef> Unwrap(const std::vector<TBlock>& blocks);
};

////////////////////////////////////////////////////////////////////////////////

class TBlockChecksumValidationException
    : public std::exception
{
public:
    TBlockChecksumValidationException(TChecksum expected, TChecksum actual)
        : Expected_(expected)
        , Actual_(actual)
    { }

    DEFINE_BYVAL_RO_PROPERTY(TChecksum, Expected);
    DEFINE_BYVAL_RO_PROPERTY(TChecksum, Actual);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

#define BLOCK_INL_H_
#include "block-inl.h"
#undef BLOCK_INL_H_
