#include "reed_solomon.h"
#include "helpers.h"
#include "jerasure.h"

#include <contrib/libs/jerasure/cauchy.h>
#include <contrib/libs/jerasure/jerasure.h>

#include <algorithm>

namespace NYT {
namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

TCauchyReedSolomon::TCauchyReedSolomon(
    int dataPartCount,
    int parityPartCount,
    int wordSize)
    : DataPartCount_(dataPartCount)
    , ParityPartCount_(parityPartCount)
    , WordSize_(wordSize)
    , Matrix_(cauchy_good_general_coding_matrix(dataPartCount, parityPartCount, wordSize))
    , BitMatrix_(jerasure_matrix_to_bitmatrix(dataPartCount, parityPartCount, wordSize, Matrix_.Get()))
    , Schedule_(jerasure_smart_bitmatrix_to_schedule(dataPartCount, parityPartCount, wordSize, BitMatrix_.Get()))
{ }

std::vector<TSharedRef> TCauchyReedSolomon::Encode(const std::vector<TSharedRef>& blocks) const
{
    return ScheduleEncode(DataPartCount_, ParityPartCount_, WordSize_, Schedule_, blocks);
}

std::vector<TSharedRef> TCauchyReedSolomon::Decode(
    const std::vector<TSharedRef>& blocks,
    const TPartIndexList& erasedIndices) const
{
    if (erasedIndices.empty()) {
        return std::vector<TSharedRef>();
    }

    return BitMatrixDecode(DataPartCount_, ParityPartCount_, WordSize_, BitMatrix_, blocks, erasedIndices);
}

TNullable<TPartIndexList> TCauchyReedSolomon::GetRepairIndices(const TPartIndexList& erasedIndices) const
{
    if (erasedIndices.empty()) {
        return TPartIndexList();
    }

    TPartIndexList indices = erasedIndices;
    std::sort(indices.begin(), indices.end());
    indices.erase(std::unique(indices.begin(), indices.end()), indices.end());

    if (indices.size() > ParityPartCount_) {
        return Null;
    }

    return Difference(0, DataPartCount_ + ParityPartCount_, indices);
}

bool TCauchyReedSolomon::CanRepair(const TPartIndexList& erasedIndices) const
{
    return erasedIndices.size() <= ParityPartCount_;
}

bool TCauchyReedSolomon::CanRepair(const TPartIndexSet& erasedIndices) const
{
    return erasedIndices.count() <= ParityPartCount_;
}

int TCauchyReedSolomon::GetDataPartCount() const
{
    return DataPartCount_;
}

int TCauchyReedSolomon::GetParityPartCount() const
{
    return ParityPartCount_;
}

int TCauchyReedSolomon::GetGuaranteedRepairablePartCount()
{
    return ParityPartCount_;
}

int TCauchyReedSolomon::GetWordSize() const
{
    return WordSize_ * 8;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT
