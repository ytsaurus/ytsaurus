#pragma once

#include "chunk_reader.h"

#include <yt/ytlib/chunk_client/reader_factory.h>

namespace NYT::NChunkClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TReaderFactoryMock
    : public IReaderFactory
{
public:
    TReaderFactoryMock(IReaderBasePtr reader):
        Reader_(std::move(reader))
    { }

    virtual IReaderBasePtr CreateReader() const override
    {
        return Reader_;
    }

    virtual bool CanCreateReader() const override
    {
        return true;
    }

    virtual const TDataSliceDescriptor& GetDataSliceDescriptor() const override
    {
        YT_UNIMPLEMENTED();
    }

private:
    IReaderBasePtr Reader_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
