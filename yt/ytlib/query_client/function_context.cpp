#include "function_context.h"

#include <cassert>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TFunctionContext::TImpl
{
public:
    TImpl(std::vector<bool> literalArgs)
        : LiteralArgs_(std::move(literalArgs))
    { }

    ~TImpl()
    {
        for (const auto& object : Objects_) {
            object.second(object.first);
        }
    }

    void RememberObjectOrDestroy(void* object, void(*deleter)(void*))
    {
        try {
            Objects_.push_back(std::make_pair(object, deleter));
        } catch(...) {
            deleter(object);
            throw;
        }
    }

    void* GetPrivateData()
    {
        return PrivateData_;
    }

    void SetPrivateData(void* data)
    {
        PrivateData_ = data;
    }

    bool IsArgLiteral(int argIndex)
    {
        assert(argIndex >= 0 && argIndex < LiteralArgs_.size());
        return LiteralArgs_[argIndex];
    }

private:
    std::vector<std::pair<void*, std::function<void(void*)>>> Objects_;
    std::vector<bool> LiteralArgs_;
    void* PrivateData_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

TFunctionContext::TFunctionContext(std::vector<bool> literalArgs)
    : Impl_(new TImpl(std::move(literalArgs)))
{ }

TFunctionContext::~TFunctionContext() = default;

bool TFunctionContext::IsArgLiteral(int argIndex)
{
    return Impl_->IsArgLiteral(argIndex);
}

void* TFunctionContext::GetPrivateData()
{
    return Impl_->GetPrivateData();
}

void TFunctionContext::SetPrivateData(void* data)
{
    Impl_->SetPrivateData(data);
}

void TFunctionContext::RememberObjectOrDestroy(void* object, void(*deleter)(void*))
{
    Impl_->RememberObjectOrDestroy(object, deleter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

