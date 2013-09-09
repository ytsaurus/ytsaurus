#pragma once

#include "public.h"

#include <core/misc/ref.h>

namespace NYT {
namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

class TSchedule
{
public:
    TSchedule();

    explicit TSchedule(int** schedulePointer);

    int** Get() const;

    TSchedule(TSchedule&&);
    TSchedule& operator = (TSchedule&&);
    
    ~TSchedule();

private:
    TSchedule(const TSchedule&);
    TSchedule operator = (const TSchedule&);

    void Free();

    int** SchedulePointer_;
};

class TMatrix
{
public:
    TMatrix();
    
    explicit TMatrix(int* matrixPointer);

    int* Get() const;
    
    TMatrix(TMatrix&&);
    TMatrix& operator = (TMatrix&&);

    ~TMatrix();

private:
    TMatrix(const TMatrix&);
    TMatrix operator = (const TMatrix&);

    void Free();

    int* MatrixPointer_;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TSharedRef> ScheduleEncode(
    int blockCount,
    int parityCount,
    int wordSize,
    const TSchedule& schedule,
    const std::vector<TSharedRef>& dataBlocks);

std::vector<TSharedRef> BitMatrixDecode(
    int blockCount,
    int parityCount,
    int wordSize,
    const TMatrix& bitTMatrix,
    const std::vector<TSharedRef>& blocks,
    const TPartIndexList& erasedIndices);

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT
