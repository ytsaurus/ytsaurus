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
    explicit TLrc(int dataPartCount);

    virtual std::vector<TSharedRef> Encode(const std::vector<TSharedRef>& blocks) const override;

    virtual std::vector<TSharedRef> Decode(
        const std::vector<TSharedRef>& blocks,
        const TPartIndexList& erasedIndices) const override;

    virtual bool CanRepair(const TPartIndexList& erasedIndices) const override;

    virtual bool CanRepair(const TPartIndexSet& erasedIndicesMask) const override;

    virtual TNullable<TPartIndexList> GetRepairIndices(const TPartIndexList& erasedIndices) const override;

    virtual int GetDataPartCount() const override;

    virtual int GetParityPartCount() const override;

    virtual int GetGuaranteedRepairablePartCount() override;

    virtual int GetWordSize() const override;

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

    bool CalculateCanRepair(const TPartIndexList& erasedIndices) const;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT

