#include "reader_factory.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TReaderFactory
    : public virtual IReaderFactory
{
public:
    TReaderFactory(
        std::function<IReaderBasePtr()> factory, 
        i64 memoryFootprint,
        const TDataSliceDescriptor& dataSliceDescriptor)
        : Factory_(factory)
        , MemoryFootprint_(memoryFootprint)
        , DataSliceDescriptor_(dataSliceDescriptor)
    { }

    virtual IReaderBasePtr CreateReader() const override
    {
        return Factory_();
    }

    virtual i64 GetMemoryFootprint() const override
    {
        return MemoryFootprint_;
    }

    virtual const TDataSliceDescriptor& GetDataSliceDescriptor() const override
    {
        return DataSliceDescriptor_;
    }

private:
    std::function<IReaderBasePtr()> Factory_;
    i64 MemoryFootprint_;
    TDataSliceDescriptor DataSliceDescriptor_;
};

////////////////////////////////////////////////////////////////////////////////

IReaderFactoryPtr CreateReaderFactory(
    std::function<IReaderBasePtr()> factory, 
    i64 memoryFootprint,
    const TDataSliceDescriptor& dataSliceDescriptor)
{
    return New<TReaderFactory>(factory, memoryFootprint, dataSliceDescriptor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
