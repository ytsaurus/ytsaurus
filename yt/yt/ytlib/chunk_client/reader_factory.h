#pragma once

#include "public.h"
#include "data_slice_descriptor.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IReaderFactory
    : public virtual TRefCounted
{
    virtual IReaderBasePtr CreateReader() const = 0;

    //! Whether reader can be created right now.
    virtual bool CanCreateReader() const = 0;

    virtual const TDataSliceDescriptor& GetDataSliceDescriptor() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IReaderFactory)

////////////////////////////////////////////////////////////////////////////////

IReaderFactoryPtr CreateReaderFactory(
    std::function<IReaderBasePtr()> factory,
    std::function<bool()> canCreateReader,
    const TDataSliceDescriptor& dataSliceDescriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
