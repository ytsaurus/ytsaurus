#include "jerasure.h"

#include <core/misc/foreach.h>

#include <contrib/libs/jerasure/jerasure.h>

namespace NYT {
namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

TSchedule::TSchedule()
    : SchedulePointer_(0)
{ }

TSchedule::TSchedule(int** schedulePointer):
    SchedulePointer_(schedulePointer)
{ }

TSchedule::TSchedule(TSchedule&& other)
{
    Free();
    SchedulePointer_ = other.SchedulePointer_;
    other.SchedulePointer_ = 0;
}

TSchedule& TSchedule::operator= (TSchedule&& other)
{
    if (this != &other) {
        Free();
        SchedulePointer_ = other.SchedulePointer_;
        other.SchedulePointer_ = 0;
    }
    return *this;
}

TSchedule::~TSchedule()
{
    Free();
}
    
void TSchedule::Free()
{
    if (SchedulePointer_ != 0) {
        jerasure_free_schedule(SchedulePointer_);
    }
}

int** TSchedule::Get() const
{
    return SchedulePointer_;
}

////////////////////////////////////////////////////////////////////////////////

TMatrix::TMatrix()
    : MatrixPointer_(0)
{ }

TMatrix::TMatrix(int* matrixPointer)
    : MatrixPointer_(matrixPointer)
{ }

TMatrix::TMatrix(TMatrix&& other)
{
    Free();
    MatrixPointer_ = other.MatrixPointer_;
    other.MatrixPointer_ = 0;
}

TMatrix& TMatrix::operator= (TMatrix&& other)
{
    if (this != &other) {
        Free();
        MatrixPointer_ = other.MatrixPointer_;
        other.MatrixPointer_ = 0;
    }
    return *this;
}

TMatrix::~TMatrix()
{ }

void TMatrix::Free()
{
    if (MatrixPointer_ != 0) {
        free(MatrixPointer_);
    }
}

int* TMatrix::Get() const
{
    return MatrixPointer_;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TSharedRef> ScheduleEncode(
    int dataCount,
    int parityCount,
    int wordSize,
    const TSchedule& schedule,
    const std::vector<TSharedRef>& dataBlocks)
{
    YCHECK(dataBlocks.size() == dataCount);
    
    i64 blockLength = dataBlocks.front().Size();
    // We should multiply WordSize by 8 because we use BitMatrix
    YCHECK(blockLength % (wordSize * 8) == 0);

    for (int i = 1; i < dataBlocks.size(); ++i) {
        YCHECK(dataBlocks[i].Size() == blockLength);
    }

    std::vector<char*> dataPointers;
    FOREACH (const auto& block, dataBlocks) {
        dataPointers.push_back(const_cast<char*>(block.Begin()));
    }
    
    std::vector<TSharedRef> parities(parityCount);
    std::vector<char*> parityPointers(parityCount);
    for (int i = 0; i < parityCount; ++i) {
        struct TJerasureTag {};
        parities[i] = TSharedRef::Allocate<TJerasureTag>(blockLength, false);
        parityPointers[i] = parities[i].Begin();
    }

    jerasure_schedule_encode(
        dataCount,
        parityCount,
        wordSize,
        schedule.Get(),
        dataPointers.data(),
        parityPointers.data(),
        blockLength,
        sizeof(long));

    return parities;
}

std::vector<TSharedRef> BitMatrixDecode(
    int blockCount,
    int parityCount,
    int wordSize,
    const TMatrix& bitMatrix,
    const std::vector<TSharedRef>& blocks,
    const TPartIndexList& erasedIndices)
{
    YCHECK(blocks.size() + erasedIndices.size() == blockCount + parityCount);
    
    i64 blockLength = blocks.front().Size();
    // We should multiply WordSize by 8 because we use BitMatrix
    YCHECK(blockLength % (wordSize * 8) == 0);

    for (int i = 1; i < blocks.size(); ++i) {
        YCHECK(blocks[i].Size() == blockLength);
    }
    
    std::vector<TSharedRef> repaired(erasedIndices.size());

    std::vector<char*> blockPointers;
    std::vector<char*> parityPointers;
    
    int blockNumber = 0;
    int erasureNumber = 0;
    for (int i = 0; i < blockCount + parityCount; ++i) {
        char* ref;
        if (erasureNumber < erasedIndices.size() && i == erasedIndices[erasureNumber]) {
            struct TJerasureTag {};
            repaired[erasureNumber] = TSharedRef::Allocate<TJerasureTag>(blockLength, false);
            ref = repaired[erasureNumber].Begin();
            erasureNumber += 1;
        } else {
            ref = const_cast<char*>(blocks[blockNumber++].Begin());
        }
        if (i < blockCount) {
            blockPointers.push_back(ref);
        } else {
            parityPointers.push_back(ref);
        }
    }
    YCHECK(erasureNumber == erasedIndices.size());
    
    auto preparedErasedIndices = erasedIndices;
    preparedErasedIndices.push_back(-1);

    YCHECK(blockPointers.size() == blockCount);
    YCHECK(parityPointers.size() == parityCount);

    int res = jerasure_schedule_decode_lazy(
        blockCount,
        parityCount,
        wordSize,
        bitMatrix.Get(),
        preparedErasedIndices.data(),
        blockPointers.data(),
        parityPointers.data(),
        blockLength,
        sizeof(long),
        1);
    YCHECK(res == 0);
    
    return repaired;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT
