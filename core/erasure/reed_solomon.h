#pragma once

#include "codec.h"
#include "jerasure.h"

namespace NYT {
namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

//! Cauchy version of the standard Reed--Solomon encoding scheme.
/*!
 *  See http://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction
 *  for more details.
 */
class TCauchyReedSolomon
    : public ICodec
{
public:
    TCauchyReedSolomon(
        int dataPartCount,
        int parityPartCount,
        int wordSize);

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

    TMatrix Matrix_;
    TMatrix BitMatrix_;
    TSchedule Schedule_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT
