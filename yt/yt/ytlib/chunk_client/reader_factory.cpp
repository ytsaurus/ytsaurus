#include "reader_factory.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TReaderFactory
    : public virtual IReaderFactory
{
public:
    TReaderFactory(
        std::function<IReaderBasePtr()> factory,
        std::function<bool()> canCreateReader,
        const TDataSliceDescriptor& dataSliceDescriptor)
        : Factory_(factory)
        , CanCreateReader_(canCreateReader)
        , DataSliceDescriptor_(dataSliceDescriptor)
    { }

    virtual IReaderBasePtr CreateReader() const override
    {
        return Factory_();
    }

    virtual bool CanCreateReader() const override
    {
        return CanCreateReader_();
    }

    virtual const TDataSliceDescriptor& GetDataSliceDescriptor() const override
    {
        return DataSliceDescriptor_;
    }

private:
    const std::function<IReaderBasePtr()> Factory_;
    const std::function<bool()> CanCreateReader_;
    const TDataSliceDescriptor DataSliceDescriptor_;
};

////////////////////////////////////////////////////////////////////////////////

IReaderFactoryPtr CreateReaderFactory(
    std::function<IReaderBasePtr()> factory,
    std::function<bool()> canCreateReader,
    const TDataSliceDescriptor& dataSliceDescriptor)
{
    return New<TReaderFactory>(factory, canCreateReader, dataSliceDescriptor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
