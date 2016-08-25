#include "reader_factory.h"

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TReaderFactory
    : public virtual IReaderFactory
{
public:
    TReaderFactory(
        std::function<IReaderBasePtr()> factory, 
        i64 memoryFootprint)
        : Factory_(factory)
        , MemoryFootprint_(memoryFootprint)
    { }

    virtual IReaderBasePtr CreateReader() const override
    {
        return Factory_();
    }

    virtual i64 GetMemoryFootprint() const override
    {
        return MemoryFootprint_;
    }

private:
    std::function<IReaderBasePtr()> Factory_;
    i64 MemoryFootprint_;
};

////////////////////////////////////////////////////////////////////////////////

IReaderFactoryPtr CreateReaderFactory(
    std::function<IReaderBasePtr()> factory, 
    i64 memoryFootprint)
{
    return New<TReaderFactory>(factory, memoryFootprint);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT