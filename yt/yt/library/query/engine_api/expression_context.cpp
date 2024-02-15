#include "expression_context.h"

#include <yt/yt/client/table_client/unversioned_row.h>

using namespace NYT::NWebAssembly;

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TExpressionContext::TExpressionContext()
    : HostPool_(New<TRowBuffer>())
    , WebAssemblyPool_(GetCurrentCompartment())
{ }

TExpressionContext::TExpressionContext(TRowBufferPtr rowBuffer)
    : HostPool_(std::move(rowBuffer))
    , WebAssemblyPool_(GetCurrentCompartment())
{ }

TExpressionContext::TExpressionContext(TExpressionContext&& other)
{
    std::swap(HostPool_, other.HostPool_);
    std::swap(WebAssemblyPool_, other.WebAssemblyPool_);
}

TExpressionContext& TExpressionContext::operator=(TExpressionContext&& other)
{
    std::swap(HostPool_, other.HostPool_);
    std::swap(WebAssemblyPool_, other.WebAssemblyPool_);
    return *this;
}

void TExpressionContext::Clear()
{
    HostPool_->Clear();
    WebAssemblyPool_.Clear();
}

void TExpressionContext::ClearWebAssemblyPool()
{
    WebAssemblyPool_.Clear();
}

i64 TExpressionContext::GetSize() const
{
    return HostPool_->GetSize() + WebAssemblyPool_.GetSize();
}

i64 TExpressionContext::GetCapacity() const
{
    return HostPool_->GetCapacity() + WebAssemblyPool_.GetCapacity();
}

char* TExpressionContext::AllocateUnaligned(size_t byteCount, NWebAssembly::EAddressSpace where)
{
    if (where == EAddressSpace::WebAssembly && WebAssemblyPool_.HasCompartment()) {
        return WebAssemblyPool_.AllocateUnaligned(byteCount);
    }
    return HostPool_->GetPool()->AllocateUnaligned(byteCount);
}

char* TExpressionContext::AllocateAligned(size_t byteCount, NWebAssembly::EAddressSpace where)
{
    if (where == EAddressSpace::WebAssembly && WebAssemblyPool_.HasCompartment()) {
        return WebAssemblyPool_.AllocateAligned(byteCount);
    }
    return HostPool_->GetPool()->AllocateAligned(byteCount);
}

TRowBufferPtr TExpressionContext::GetRowBuffer() const
{
    return HostPool_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
