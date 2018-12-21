#include "jerasure.h"

#include <yt/core/concurrency/fork_aware_spinlock.h>

extern "C" {
#include <yt/contrib/jerasure/jerasure.h>
}

namespace NYT::NErasure {

////////////////////////////////////////////////////////////////////////////////

TSchedule::TSchedule()
    : SchedulePointer_(nullptr)
{ }

TSchedule::TSchedule(int** schedulePointer):
    SchedulePointer_(schedulePointer)
{ }

TSchedule::TSchedule(TSchedule&& other)
{
    Free();
    SchedulePointer_ = other.SchedulePointer_;
    other.SchedulePointer_ = nullptr;
}

TSchedule& TSchedule::operator= (TSchedule&& other)
{
    if (this != &other) {
        Free();
        SchedulePointer_ = other.SchedulePointer_;
        other.SchedulePointer_ = nullptr;
    }
    return *this;
}

TSchedule::~TSchedule()
{
    Free();
}
    
void TSchedule::Free()
{
    if (SchedulePointer_) {
        jerasure_free_schedule(SchedulePointer_);
        SchedulePointer_ = nullptr;
    }
}

int** TSchedule::Get() const
{
    return SchedulePointer_;
}

////////////////////////////////////////////////////////////////////////////////

TMatrix::TMatrix()
    : MatrixPointer_(nullptr)
{ }

TMatrix::TMatrix(int* matrixPointer)
    : MatrixPointer_(matrixPointer)
{ }

TMatrix::TMatrix(TMatrix&& other)
{
    Free();
    MatrixPointer_ = other.MatrixPointer_;
    other.MatrixPointer_ = nullptr;
}

TMatrix& TMatrix::operator= (TMatrix&& other)
{
    if (this != &other) {
        Free();
        MatrixPointer_ = other.MatrixPointer_;
        other.MatrixPointer_ = nullptr;
    }
    return *this;
}

TMatrix::~TMatrix()
{
    Free();
}

void TMatrix::Free()
{
    if (MatrixPointer_) {
        free(MatrixPointer_);
        MatrixPointer_ = nullptr;
    }
}

int* TMatrix::Get() const
{
    return MatrixPointer_;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

std::atomic<bool> JerasureInitialized = {false};
NConcurrency::TForkAwareSpinLock JerasureInitLock;

} // namespace

void InitializeJerasure()
{
    if (JerasureInitialized.load(std::memory_order_relaxed)) {
        return;
    }

    auto guard = Guard(JerasureInitLock);

    if (!JerasureInitialized.load()) {
        // Cf. galois.c.
        for (int w = 1; w <= MaxWordSize; ++w) {
            galois_create_log_tables(w);
        }
        for (int w = 1; w <= MaxWordSize; ++w) {
            galois_create_mult_tables(w);
        }

        JerasureInitialized.store(true);
    }
}

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
    for (const auto& block : dataBlocks) {
        dataPointers.push_back(const_cast<char*>(block.Begin()));
    }
    
    std::vector<TSharedMutableRef> parities(parityCount);
    std::vector<char*> parityPointers(parityCount);
    for (int i = 0; i < parityCount; ++i) {
        struct TJerasureTag {};
        parities[i] = TSharedMutableRef::Allocate<TJerasureTag>(blockLength, false);
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

    return std::vector<TSharedRef>(parities.begin(), parities.end());
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
    
    std::vector<TSharedMutableRef> repaired(erasedIndices.size());

    std::vector<char*> blockPointers;
    std::vector<char*> parityPointers;
    
    int blockNumber = 0;
    int erasureNumber = 0;
    for (int i = 0; i < blockCount + parityCount; ++i) {
        char* ptr;
        if (erasureNumber < erasedIndices.size() && i == erasedIndices[erasureNumber]) {
            struct TJerasureTag {};
            repaired[erasureNumber] = TSharedMutableRef::Allocate<TJerasureTag>(blockLength, false);
            ptr = repaired[erasureNumber].Begin();
            erasureNumber += 1;
        } else {
            ptr = const_cast<char*>(blocks[blockNumber++].Begin());
        }
        if (i < blockCount) {
            blockPointers.push_back(ptr);
        } else {
            parityPointers.push_back(ptr);
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
    
    return std::vector<TSharedRef>(repaired.begin(), repaired.end());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NErasure
