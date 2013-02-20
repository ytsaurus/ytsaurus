#pragma once

#include "codec.h"
#include "jerasure.h"

namespace NYT {

namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

class TCauchyReedSolomon
    : public ICodec
{
public:
    TCauchyReedSolomon(int blockCount, int parityCount, int wordSize);

    virtual std::vector<TSharedRef> Encode(const std::vector<TSharedRef>& blocks) const override;

    virtual std::vector<TSharedRef> Decode(
        const std::vector<TSharedRef>& blocks,
        const std::vector<int>& erasedIndices) const override;

    virtual TNullable<std::vector<int>> GetRecoveryIndices(const std::vector<int>& erasedIndices) const override;

    virtual int GetDataBlockCount() const override;

    virtual int GetParityBlockCount() const override;

    virtual int GetWordSize() const override;

private:
    int BlockCount_;
    int ParityCount_;
    int WordSize_;

    TMatrix Matrix_;
    TMatrix BitMatrix_;
    TSchedule Schedule_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure

} // namespace NYT
