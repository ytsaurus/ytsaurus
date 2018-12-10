#pragma once

#include "public.h"

#include <yt/core/misc/ref.h>

namespace NYT::NErasure {

////////////////////////////////////////////////////////////////////////////////

class TSchedule
{
public:
    TSchedule();
    explicit TSchedule(int** schedulePointer);
    TSchedule(const TSchedule&& other) = delete;

    int** Get() const;

    TSchedule(TSchedule&&);
    TSchedule& operator = (TSchedule&&);
    TSchedule operator = (const TSchedule&) = delete;

    ~TSchedule();

private:
    int** SchedulePointer_;

    void Free();

};

////////////////////////////////////////////////////////////////////////////////

class TMatrix
{
public:
    TMatrix();
    explicit TMatrix(int* matrixPointer);
    TMatrix(const TMatrix&) = delete;

    int* Get() const;
    
    TMatrix(TMatrix&&);
    TMatrix& operator = (TMatrix&&);
    TMatrix operator = (const TMatrix&) = delete;

    ~TMatrix();

private:
    int* MatrixPointer_;

    void Free();

};

////////////////////////////////////////////////////////////////////////////////

const int MaxWordSize = 8;

//! Must be invoked prior to calling jerasure or galois functions to ensure
//! thread-safe initialization.
/*!
 *  Note that the tables are only initialized for word sizes up to #MaxWordSize.
 *  Don't use sizes exceeding this limit without adjusting the constant.
 */
void InitializeJerasure();

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

} // namespace NYT::NErasure
