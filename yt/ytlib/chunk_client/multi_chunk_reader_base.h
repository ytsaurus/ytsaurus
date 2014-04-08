#pragma once

#include "public.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////\

class TNontemplateMultiChunkReaderBase
    : public virtual IMultiChunkReader
{
public:
    TMultiChunkReaderBase();

    virtual TAsyncError Open() override;

    virtual TAsyncError GetReadyEvent() override;

    virtual bool IsFetchingComplete() const override;

    virtual NProto::TDataStatistics GetDataStatistics() const override;

    virtual std::vector<TChunkId> GetFailedChunkIds() const override;

protected:

};

////////////////////////////////////////////////////////////////////////////////

class TNontemplateSequentialMultiChunkReaderBase
{
public:

private:
};

////////////////////////////////////////////////////////////////////////////////

class TNontemplateParallelMultiChunkReaderBase
{
public:

private:
};

////////////////////////////////////////////////////////////////////////////////

template <class TBase, class TReader>
class TMultiChunkReaderBase
{
    
};

////////////////////////////////////////////////////////////////////////////////


bool Read(std::vector<TUnversionedRow>* rows)
{
    auto hasMore = CurrentReader->Read(rows);
    if (!rows->empty()) {
        return true;
    }

    if (hasMore) {
        OnReaderBlocked();
        return true;
    } else {
        OnReaderEnded();
        return HasMoreData();
    }
}


} // namespace NChunkClient
} // namespace NYT