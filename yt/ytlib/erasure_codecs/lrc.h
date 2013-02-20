#pragma once

#include "codec.h"
#include "jerasure.h"

namespace NYT {
namespace NErasure {

///////////////////////////////////////////////////////////////////////////////

//! Local Reconstructable Codes developed in MSR.
//! We divide data blocks into two equal parts. For each part we construct parity block.
//! Additionally we construct two RS parity blocks independent of xor blocks.
//! Details in https://www.usenix.org/conference/usenixfederatedconferencesweek/erasure-coding-windows-azure-storage
class TLrc
    : public ICodec
{
public:
    explicit TLrc(int blockCount);
    
    virtual std::vector<TSharedRef> Encode(const std::vector<TSharedRef>& blocks) const override;

    virtual std::vector<TSharedRef> Decode(
        const std::vector<TSharedRef>& blocks,
        const std::vector<int>& erasedIndices) const override;

    virtual TNullable<std::vector<int>> GetRecoveryIndices(const std::vector<int>& erasedIndices) const override;
    
    virtual int GetDataBlockCount() const override;

    virtual int GetParityBlockCount() const override;
    
    virtual int GetWordSize() const override;

private:
    int BlockCount_;
    int ParityCount_;
    int WordSize_;

    std::vector<int> Matrix_;
    TMatrix BitMatrix_;
    TSchedule Schedule_;

    // Indices of data blocks and corresponding xor (we have two xor parities).
    std::vector<int> Groups_[2];
};
    
///////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT

