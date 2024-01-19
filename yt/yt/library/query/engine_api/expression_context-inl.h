#ifndef EXPRESSION_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include expression_context.h"
// For the sake of sane code completion.
#include "expression_context.h"
#endif

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class TTag>
TExpressionContext MakeExpressionContext(TTag, size_t startChunkSize)
{
    return TExpressionContext(New<TRowBuffer>(TTag(), startChunkSize));
}

template <class TTag>
TExpressionContext MakeExpressionContext(TTag, IMemoryChunkProviderPtr chunkProvider)
{
    return TExpressionContext(New<TRowBuffer>(GetRefCountedTypeCookie<TTag>(), std::move(chunkProvider)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
