#include "reed_solomon.h"

#include <ytlib/misc/foreach.h>

#include <contrib/libs/jerasure/cauchy.h>
#include <contrib/libs/jerasure/jerasure.h>

namespace NYT {

namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

Schedule::Schedule(int** schedulePointer):
    SchedulePointer_(schedulePointer)
{ }

Schedule::~Schedule()
{
    jerasure_free_schedule(SchedulePointer_);
}

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
    YCHECK(blocks.size() == BlockCount_);
    
    i64 blockLength = blocks.front().Size();
    YCHECK(blockLength % WordSize_ == 0);

    for (int i = 1; i < blocks.size(); ++i) {
        YCHECK(blocks[i].Size() == blockLength);
    }

    std::vector<char*> blockPointers;
    FOREACH(const auto& block, blocks) {
        blockPointers.push_back(const_cast<char*>(block.Begin()));
    }
    
    std::vector<TSharedRef> parities;
    std::vector<char*> parityPointers;
    for (int i = 0; i < BlockCount_; ++i) {
        parities.push_back(TSharedRef(blockLength));
        parityPointers.push_back(static_cast<char*>(parities[i].Begin()));
    }

    jerasure_schedule_encode(
        BlockCount_,
        ParityCount_,
        WordSize_,
        Schedule_.Get(),
        blockPointers.data(),
        parityPointers.data(),
        blockLength,
        sizeof(long));

    return parities;
}

std::vector<TSharedRef> TCauchyReedSolomon::Decode(
    const std::vector<TSharedRef>& blocks,
    const std::vector<int>& erasedIndices)
{
    if (erasedIndices.empty()) {
        return std::vector<TSharedRef>();
    }

    YCHECK(blocks.size() == BlockCount_);
    
    i64 blockLength = blocks.front().Size();
    YCHECK(blockLength % WordSize_ == 0);

    for (int i = 1; i < blocks.size(); ++i) {
        YCHECK(blocks[i].Size() == blockLength);
    }

    std::vector<TSharedRef> repaired;

    std::vector<char*> blockPointers;
    std::vector<char*> parityPointers;
    
    int blockNumber = 0;
    int erasureNumber = 0;
    for (int i = 0; i < BlockCount_ + ParityCount_; ++i) {
        char* ref;
        if (erasureNumber < erasedIndices.size() && i == erasedIndices[erasureNumber]) {
            repaired.push_back(TSharedRef(blockLength));
            ref = repaired.back().Begin();
            erasureNumber += 1;
        }
        else {
            ref = const_cast<char*>(blocks[blockNumber++].Begin());
        }
        if (i < BlockCount_) {
            blockPointers.push_back(ref);
        }
        else {
            parityPointers.push_back(ref);
        }
    }
    
    std::vector<int> preparedErasedIndices = erasedIndices;
    preparedErasedIndices.push_back(-1);

    jerasure_schedule_decode_lazy(
        BlockCount_,
        ParityCount_,
        WordSize_,
        BitMatrix_.Get(),
        preparedErasedIndices.data(),
        blockPointers.data(),
        parityPointers.data(),
        blockLength,
        sizeof(long),
        1);
    
    return repaired;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure

} // namespace NYT
