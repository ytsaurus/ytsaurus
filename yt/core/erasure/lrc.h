#pragma once

#include "codec.h"
#include "jerasure.h"

namespace NYT {
namespace NErasure {

///////////////////////////////////////////////////////////////////////////////

//! Locally Reconstructible Codes
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

    virtual std::vector<TSharedRef> Decode(
        const std::vector<TSharedRef>& blocks,
        const TPartIndexList& erasedIndices) override;

    virtual bool CanRepair(const TPartIndexList& erasedIndices) override;

    virtual bool CanRepair(const TPartIndexSet& erasedIndicesMask) override;

    virtual TNullable<TPartIndexList> GetRepairIndices(const TPartIndexList& erasedIndices) override;

    virtual int GetDataPartCount() override;

    virtual int GetParityPartCount() override;

    virtual int GetWordSize() override;

private:
    int DataPartCount_;
    int ParityPartCount_;
    int WordSize_;

    TPartIndexList Matrix_;
    TMatrix BitMatrix_;
    TSchedule Schedule_;

    // Indices of data blocks and corresponding xor (we have two xor parities).
    TPartIndexList Groups_[2];

    static const int BitmaskOptimizationThreshold;
    std::vector<bool> CanRepair_;

    bool CalculateCanRepair(const TPartIndexList& erasedIndices);
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT

