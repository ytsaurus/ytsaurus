#pragma once

#include "chunk_reader.h"

#include <yt/yt/ytlib/chunk_client/reader_factory.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TReaderFactoryMock
    : public IReaderFactory
{
public:
    TReaderFactoryMock(IReaderBasePtr reader):
        Reader_(std::move(reader))
    { }

    TFuture<IReaderBasePtr> CreateReader() const override
    {
        return MakeFuture(Reader_);
    }

    bool CanCreateReader() const override
    {
        return true;
    }

    const TDataSliceDescriptor& GetDataSliceDescriptor() const override
    {
        YT_UNIMPLEMENTED();
    }

private:
    IReaderBasePtr Reader_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
