#include "reader_factory.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TReaderFactory
    : public virtual IReaderFactory
{
public:
    TReaderFactory(
        TCallback<TFuture<IReaderBasePtr>()> factory,
        TCallback<bool()> canCreateReader,
        const TDataSliceDescriptor& dataSliceDescriptor)
        : Factory_(std::move(factory))
        , CanCreateReader_(std::move(canCreateReader))
        , DataSliceDescriptor_(dataSliceDescriptor)
    { }

    TFuture<IReaderBasePtr> CreateReader() const override
    {
        return Factory_();
    }

    bool CanCreateReader() const override
    {
        return CanCreateReader_();
    }

    const TDataSliceDescriptor& GetDataSliceDescriptor() const override
    {
        return DataSliceDescriptor_;
    }

private:
    const TCallback<TFuture<IReaderBasePtr>()> Factory_;
    const TCallback<bool()> CanCreateReader_;
    const TDataSliceDescriptor DataSliceDescriptor_;
};

////////////////////////////////////////////////////////////////////////////////

IReaderFactoryPtr CreateReaderFactory(
    TCallback<IReaderBasePtr()> factory,
    TCallback<bool()> canCreateReader,
    const TDataSliceDescriptor& dataSliceDescriptor)
{
    return CreateReaderFactory(
        BIND([factory = std::move(factory)] { return MakeFuture(factory()); }),
        std::move(canCreateReader),
        dataSliceDescriptor);
}

IReaderFactoryPtr CreateReaderFactory(
    TCallback<TFuture<IReaderBasePtr>()> factory,
    TCallback<bool()> canCreateReader,
    const TDataSliceDescriptor& dataSliceDescriptor)
{
    return New<TReaderFactory>(
        std::move(factory),
        std::move(canCreateReader),
        dataSliceDescriptor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
