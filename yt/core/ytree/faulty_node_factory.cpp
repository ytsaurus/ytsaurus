#include "faulty_node_factory.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

struct TFaultyNodeFactory
    : public INodeFactory
{
#define IMPLEMENT_CREATE_METHOD(name) \
    virtual I##name##NodePtr Create##name() override \
    { \
        ThrowInvalidType(ENodeType::name); \
        Y_UNREACHABLE(); \
    } \

    IMPLEMENT_CREATE_METHOD(String)
    IMPLEMENT_CREATE_METHOD(Int64)
    IMPLEMENT_CREATE_METHOD(Uint64)
    IMPLEMENT_CREATE_METHOD(Double)
    IMPLEMENT_CREATE_METHOD(Boolean)
    IMPLEMENT_CREATE_METHOD(List)
    IMPLEMENT_CREATE_METHOD(Map)
    IMPLEMENT_CREATE_METHOD(Entity)
#undef IMPLEMENT_CREATE_METHOD

private:
    void ThrowInvalidType(NYTree::ENodeType type)
    {
        THROW_ERROR_EXCEPTION("Cannot create a node of the %Qv type", type);
    }
};

std::unique_ptr<INodeFactory> CreateFaultyNodeFactory()
{
    return std::unique_ptr<INodeFactory>(new TFaultyNodeFactory());
}

INodeFactory* GetFaultyNodeFactory()
{
    static auto faultyFactory = CreateFaultyNodeFactory();
    return faultyFactory.get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

