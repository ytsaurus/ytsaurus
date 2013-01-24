#pragma once

#include "codec.h"

namespace NYT {

namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

class Schedule
{
public:
    Schedule(int** schedule);

    int** Get() const;

    ~Schedule();

private:
    int** SchedulePointer_;
};

////////////////////////////////////////////////////////////////////////////////

class TCauchyReedSolomon
    : public ICodec
{
public:
    TCauchyReedSolomon(int blockCount, int parityCount, int wordSize);
    
    virtual std::vector<TSharedRef> Encode(const std::vector<TSharedRef>& blocks);

    virtual std::vector<TSharedRef> Decode(
        const std::vector<TSharedRef>& blocks,
        const std::vector<int>& erasedIndices);

private:
    int BlockCount_;
    int ParityCount_;
    int WordSize_;
    
    TAutoPtr<int> Matrix_;
    TAutoPtr<int> BitMatrix_;
    Schedule Schedule_;
};
    
////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure

} // namespace NYT
