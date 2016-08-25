#include "function_context.h"

#include <cassert>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TFunctionContext::TFunctionContext(std::vector<bool> literalArgs)
    : LiteralArgs_(std::move(literalArgs))
{ }

TFunctionContext::~TFunctionContext() = default;

bool TFunctionContext::IsArgLiteral(int argIndex)
{
    assert(argIndex >= 0 && argIndex < LiteralArgs_.size());
        return LiteralArgs_[argIndex];
}

void* TFunctionContext::GetPrivateData()
{
    return PrivateData_;
}

void TFunctionContext::SetPrivateData(void* data)
{
    PrivateData_ = data;
}

void TFunctionContext::RememberObjectOrDestroy(void* object, void(*deleter)(void*))
{
    Objects_.push_back(std::unique_ptr<void, void(*)(void*)>(object, deleter));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

