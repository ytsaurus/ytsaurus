#pragma once

#include <ytlib/misc/ref.h>
#include <ytlib/misc/nullable.h>

namespace NYT {
namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ECodec,
    ((None)           (0))
    ((ReedSolomon_6_3)(1))
    ((Lrc_12_2_2)     (2))
);

////////////////////////////////////////////////////////////////////////////////

struct ICodec
{
    virtual std::vector<TSharedRef> Encode(const std::vector<TSharedRef>& blocks) const = 0;

    virtual std::vector<TSharedRef> Decode(const std::vector<TSharedRef>& blocks, const std::vector<int>& erasedIndices) const = 0;

    virtual TNullable<std::vector<int>> GetRecoveryIndices(const std::vector<int>& erasedIndices) const = 0;

    virtual int GetDataBlockCount() const = 0;

    virtual int GetParityBlockCount() const = 0;

    virtual int GetTotalBlockCount() const;

    virtual int GetWordSize() const = 0;
};


//! Returns a codec for the registered id.
ICodec* GetCodec(ECodec id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT


