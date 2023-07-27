#pragma once

#include "lrc.h"

#include "helpers.h"
#include "jerasure.h"

extern "C" {
#include <contrib/libs/jerasure/jerasure.h>
#include <contrib/libs/jerasure/cauchy.h>
}

#include <library/cpp/sse/sse.h>

#include <util/generic/array_ref.h>

#include <algorithm>
#include <optional>

namespace NErasure {

//! Locally Reconstructable Codes
/*!
 *  See https://www.usenix.org/conference/usenixfederatedconferencesweek/erasure-coding-windows-azure-storage
 *  for more details.
 */
template <int DataPartCount, int ParityPartCount, int WordSize, class TCodecTraits>
class TLrcJerasure
    : public TLrcCodecBase<DataPartCount, ParityPartCount, WordSize, TCodecTraits>
{
public:
    //! Main blob for storing data.
    using TBlobType = typename TCodecTraits::TBlobType;
    //! Main mutable blob for decoding data.
    using TMutableBlobType = typename TCodecTraits::TMutableBlobType;

    static constexpr ui64 RequiredDataAlignment = alignof(ui64);

    TLrcJerasure()
        : TLrcCodecBase<DataPartCount, ParityPartCount, WordSize, TCodecTraits>()
    {
        InitializeJerasure();

        GeneratorMatrix_.resize(ParityPartCount * DataPartCount);
        this->template InitializeGeneratorMatrix<typename decltype(GeneratorMatrix_)::value_type>(
            GeneratorMatrix_.data(),
            std::bind(&galois_single_multiply, std::placeholders::_1, std::placeholders::_1, WordSize));

        BitMatrix_ = TJerasureMatrixHolder(jerasure_matrix_to_bitmatrix(DataPartCount, ParityPartCount, WordSize, GeneratorMatrix_.data()));
        Schedule_ = TJerasureScheduleHolder(jerasure_dumb_bitmatrix_to_schedule(DataPartCount, ParityPartCount, WordSize, BitMatrix_.Get()));
    }

    std::vector<TBlobType> Encode(const std::vector<TBlobType>& blocks) const override {
        return ScheduleEncode<DataPartCount, ParityPartCount, WordSize, TCodecTraits>(Schedule_, blocks);
    }

    virtual ~TLrcJerasure() = default;

private:
    std::vector<TBlobType> FallbackToCodecDecode(
        const std::vector<TBlobType>& blocks,
        TPartIndexList erasedIndices) const override
    {
        // Choose subset of matrix rows, corresponding for erased and recovery indices.
        int parityCount = blocks.size() + erasedIndices.size() - DataPartCount;
        auto recoveryIndices = this->GetRepairIndices(erasedIndices).value();

        TPartIndexList rows;
        for (int ind : recoveryIndices) {
            if (ind >= DataPartCount) {
                rows.push_back(ind - DataPartCount);
            }
        }

        for (int& ind : erasedIndices) {
            if (ind >= DataPartCount) {
                rows.push_back(ind - DataPartCount);
                ind = DataPartCount + rows.size() - 1;
            }
        }

        TPartIndexList matrix = ExtractRows(GeneratorMatrix_, DataPartCount, rows);
        TJerasureMatrixHolder bitMatrix = TJerasureMatrixHolder(jerasure_matrix_to_bitmatrix(DataPartCount, parityCount, WordSize, matrix.data()));

        return BitMatrixDecode<DataPartCount, WordSize, TCodecTraits>(parityCount, bitMatrix, blocks, erasedIndices);
    }

    // For GeneratorMatrix_, BitMatrix_ and Schedule_ meaning see reed_solomon.h.
    TPartIndexList GeneratorMatrix_;
    TJerasureMatrixHolder BitMatrix_;
    TJerasureScheduleHolder Schedule_;
};

} // namespace NErasure
