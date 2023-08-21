#pragma once

#include "public.h"
#include "data_slice_descriptor.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IReaderFactory
    : public virtual TRefCounted
{
    virtual TFuture<IReaderBasePtr> CreateReader() const = 0;
    virtual bool CanCreateReader() const = 0;

    virtual const TDataSliceDescriptor& GetDataSliceDescriptor() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IReaderFactory)

////////////////////////////////////////////////////////////////////////////////

IReaderFactoryPtr CreateReaderFactory(
    TCallback<IReaderBasePtr()> factory,
    TCallback<bool()> canCreateReader,
    const TDataSliceDescriptor& dataSliceDescriptor);

IReaderFactoryPtr CreateReaderFactory(
    TCallback<TFuture<IReaderBasePtr>()> factory,
    TCallback<bool()> canCreateReader,
    const TDataSliceDescriptor& dataSliceDescriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
