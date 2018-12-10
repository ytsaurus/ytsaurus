#pragma once

#include "public.h"
#include "data_slice_descriptor.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IReaderFactory
    : public virtual TRefCounted
{
    virtual i64 GetMemoryFootprint() const = 0;

    virtual IReaderBasePtr CreateReader() const = 0;

    virtual const TDataSliceDescriptor& GetDataSliceDescriptor() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IReaderFactory)

////////////////////////////////////////////////////////////////////////////////

IReaderFactoryPtr CreateReaderFactory(
    std::function<IReaderBasePtr()> factory, 
    i64 memoryFootprint,
    const TDataSliceDescriptor& dataSliceDescriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
