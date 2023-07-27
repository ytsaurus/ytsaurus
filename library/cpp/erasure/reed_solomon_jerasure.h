#pragma once

#include "jerasure.h"
#include "reed_solomon.h"

extern "C" {
#include <contrib/libs/jerasure/cauchy.h>
#include <contrib/libs/jerasure/jerasure.h>
}

namespace NErasure {

//! Cauchy version of the standard Reed--Solomon encoding scheme.
/*!
 *  See http://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction
 *  for more details.
 */
template <int DataPartCount, int ParityPartCount, int WordSize, class TCodecTraits>
class TCauchyReedSolomonJerasure
    : public TReedSolomonBase<DataPartCount, ParityPartCount, WordSize, TCodecTraits>
{
public:
    //! Main blob for storing data.
    using TBlobType = typename TCodecTraits::TBlobType;

    TCauchyReedSolomonJerasure() {
        InitializeJerasure();
        Matrix_ = TJerasureMatrixHolder(cauchy_good_general_coding_matrix(DataPartCount, ParityPartCount, WordSize));
        BitMatrix_ = TJerasureMatrixHolder(jerasure_matrix_to_bitmatrix(DataPartCount, ParityPartCount, WordSize, Matrix_.Get()));
        Schedule_ = TJerasureScheduleHolder(jerasure_smart_bitmatrix_to_schedule(DataPartCount, ParityPartCount, WordSize, BitMatrix_.Get()));
    }

    virtual std::vector<TBlobType> Encode(const std::vector<TBlobType>& blocks) const override {
        return ScheduleEncode<DataPartCount, ParityPartCount, WordSize, TCodecTraits>(Schedule_, blocks);
    }

    virtual std::vector<TBlobType> Decode(
        const std::vector<TBlobType>& blocks,
        const TPartIndexList& erasedIndices) const override
    {
        if (erasedIndices.empty()) {
            return std::vector<TBlobType>();
        }

        return BitMatrixDecode<DataPartCount, WordSize, TCodecTraits>(ParityPartCount, BitMatrix_, blocks, erasedIndices);
    }

    virtual ~TCauchyReedSolomonJerasure() = default;

private:
    // Matrix to encode data
    TJerasureMatrixHolder Matrix_;

    /*
     * BitMatrix, actually it is WordSize^2 bigger than the Matrix_ and the k-th column is
     * the binary representation of 2^k * Matrix_[i][j] in Galois Filed 2^WordSize
     */
    TJerasureMatrixHolder BitMatrix_;

    /*
     * Schedule is a lazy operation for matrix inversion and encoding
     */
    TJerasureScheduleHolder Schedule_;
};

} // namespace NErasure
