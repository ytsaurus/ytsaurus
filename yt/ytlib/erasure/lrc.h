#pragma once

#include "codec.h"
#include "jerasure.h"

namespace NYT {
namespace NErasure {

///////////////////////////////////////////////////////////////////////////////

//! Locally Reconstructable Codes 
/*!
 *  See https://www.usenix.org/conference/usenixfederatedconferencesweek/erasure-coding-windows-azure-storage
 *  for more details.
 */
class TLrc
    : public ICodec
{
public:
    explicit TLrc(int blockCount);
    
    virtual std::vector<TSharedRef> Encode(const std::vector<TSharedRef>& blocks) override;

    virtual bool CanRepair(const TBlockIndexList& erasedIndices) override
    {
        // TODO(babenko): move to cpp and fixme
        return true;
    }

    virtual std::vector<TSharedRef> Decode(
        const std::vector<TSharedRef>& blocks,
        const TBlockIndexList& erasedIndices) override;

    virtual TNullable<TBlockIndexList> GetRepairIndices(const TBlockIndexList& erasedIndices) override;
    
    virtual int GetDataBlockCount() override;

    virtual int GetParityBlockCount() override;
    
    virtual int GetWordSize() override;

private:
    int BlockCount_;
    int ParityCount_;
    int WordSize_;

    TBlockIndexList Matrix_;
    TMatrix BitMatrix_;
    TSchedule Schedule_;

    // Indices of data blocks and corresponding xor (we have two xor parities).
    TBlockIndexList Groups_[2];

};
    
///////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT

