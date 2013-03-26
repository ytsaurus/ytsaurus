#include "reed_solomon.h"
#include "helpers.h"
#include "jerasure.h"

#include <ytlib/misc/foreach.h>

#include <contrib/libs/jerasure/cauchy.h>
#include <contrib/libs/jerasure/jerasure.h>

#include <algorithm>

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

std::vector<TSharedRef> TCauchyReedSolomon::Encode(const std::vector<TSharedRef>& blocks)
{
    return ScheduleEncode(BlockCount_, ParityCount_, WordSize_, Schedule_, blocks);
}

std::vector<TSharedRef> TCauchyReedSolomon::Decode(
    const std::vector<TSharedRef>& blocks,
    const TBlockIndexList& erasedIndices)
{
    if (erasedIndices.empty()) {
        return std::vector<TSharedRef>();
    }
    
    return BitMatrixDecode(BlockCount_, ParityCount_, WordSize_, BitMatrix_, blocks, erasedIndices);
}

TNullable<TBlockIndexList> TCauchyReedSolomon::GetRepairIndices(const TBlockIndexList& erasedIndices)
{
    if (erasedIndices.empty()) {
        return TBlockIndexList();
    }
    
    TBlockIndexList indices = erasedIndices;
    std::sort(indices.begin(), indices.end());
    indices.erase(std::unique(indices.begin(), indices.end()), indices.end());
    
    if (indices.size() > ParityCount_) {
        return Null;
    }

    return Difference(0, BlockCount_ + ParityCount_, indices);
}

int TCauchyReedSolomon::GetDataBlockCount()
{
    return BlockCount_;
}

int TCauchyReedSolomon::GetParityBlockCount()
{
    return ParityCount_;
}

int TCauchyReedSolomon::GetWordSize()
{
    return WordSize_ * 8;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure

} // namespace NYT
