#include "reed_solomon.h"
#include "helpers.h"
#include "jerasure.h"

#include <ytlib/misc/foreach.h>

#include <contrib/libs/jerasure/cauchy.h>
#include <contrib/libs/jerasure/jerasure.h>

#include <iostream>

namespace NYT {

namespace NErasure {

////////////////////////////////////////////////////////////////////////////////
    
TCauchyReedSolomon::TCauchyReedSolomon(int blockCount, int parityCount, int wordSize)
    : BlockCount_(blockCount)
    , ParityCount_(parityCount)
    , WordSize_(wordSize)
    , Matrix_(cauchy_good_general_coding_matrix(blockCount, parityCount, wordSize))
    , BitMatrix_(jerasure_matrix_to_bitmatrix(blockCount, parityCount, wordSize, Matrix_.Get()))
    , Schedule_(jerasure_smart_bitmatrix_to_schedule(blockCount, parityCount, wordSize, BitMatrix_.Get()))
{ }

std::vector<TSharedRef> TCauchyReedSolomon::Encode(const std::vector<TSharedRef>& blocks) const
{
    return ScheduleEncode(BlockCount_, ParityCount_, WordSize_, Schedule_, blocks);
}

std::vector<TSharedRef> TCauchyReedSolomon::Decode(
    const std::vector<TSharedRef>& blocks,
    const std::vector<int>& erasedIndices) const
{
    if (erasedIndices.empty()) {
        return std::vector<TSharedRef>();
    }
    
    return BitMatrixDecode(BlockCount_, ParityCount_, WordSize_, BitMatrix_, blocks, erasedIndices);
}

TNullable<std::vector<int>> TCauchyReedSolomon::GetRecoveryIndices(const std::vector<int>& erasedIndices) const
{
    if (erasedIndices.empty()) {
        return std::vector<int>();
    }
    
    std::vector<int> indices = erasedIndices;
    sort(indices.begin(), indices.end());
    indices.erase(unique(indices.begin(), indices.end()), indices.end());
    
    if (indices.size() > ParityCount_) {
        return Null;
    }

    return Difference(0, BlockCount_ + ParityCount_, indices);
}

int TCauchyReedSolomon::GetDataBlockCount() const
{
    return BlockCount_;
}

int TCauchyReedSolomon::GetParityBlockCount() const
{
    return ParityCount_;
}

int TCauchyReedSolomon::GetWordSize() const
{
    return WordSize_ * 8;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure

} // namespace NYT
