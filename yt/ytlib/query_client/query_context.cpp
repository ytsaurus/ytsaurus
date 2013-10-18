#include "query_context.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

static const size_t InitialMemoryPoolSize = 4096;

////////////////////////////////////////////////////////////////////////////////

TQueryContext::TTrackedObject::TTrackedObject(TQueryContext* context)
    : Context_(context)
{
    Context_->TrackedObjects_.insert(this);
}

TQueryContext::TTrackedObject::~TTrackedObject()
{
    Context_ = nullptr;
}

void* TQueryContext::TTrackedObject::operator new(
    size_t bytes,
    TQueryContext* context)
{
    return context->Allocate(bytes);
}

void TQueryContext::TTrackedObject::operator delete(
    void* pointer,
    TQueryContext* context) throw()
{
    context->Deallocate(pointer);
}

void* TQueryContext::TTrackedObject::operator new(size_t)
{
    YUNREACHABLE();
}

void TQueryContext::TTrackedObject::operator delete(void*) throw()
{
    YUNREACHABLE();
}

TQueryContext::TQueryContext()
    : MemoryPool_(InitialMemoryPoolSize)
{ }

TQueryContext::~TQueryContext()
{
    FOREACH (auto& object, TrackedObjects_) {
        object->~TTrackedObject();
        TTrackedObject::operator delete(object, this);
    }
}

void* TQueryContext::Allocate(size_t size)
{
    return MemoryPool_.Allocate(size);
}

void TQueryContext::Deallocate(void*)
{ }

TStringBuf TQueryContext::Capture(const char* begin, const char* end)
{
    return TStringBuf(MemoryPool_.Append(begin, end - begin), end - begin);
}

void TQueryContext::SetDebugInformation(TDebugInformation&& debugInformation)
{
    DebugInformation_ = std::move(debugInformation);
}

const TDebugInformation* TQueryContext::GetDebugInformation() const
{
    return DebugInformation_.GetPtr();
}

int TQueryContext::GetTableIndexByAlias(const TStringBuf& alias)
{
    auto begin = TableDescriptors_.begin();
    auto end = TableDescriptors_.end();

    auto it = std::find_if(
        begin,
        end,
        [&alias] (const TTableDescriptor& descriptor) {
            return descriptor.Alias == alias;
        });

    if (it == end) {
        it = TableDescriptors_.insert(it, TTableDescriptor());
        it->Alias = alias;
        it->Opaque = nullptr;

        begin = TableDescriptors_.begin();
        end = TableDescriptors_.end();
    }

    return std::distance(begin, it);
}

TTableDescriptor& TQueryContext::GetTableDescriptorByIndex(int tableIndex)
{
    return TableDescriptors_[tableIndex];
}

void TQueryContext::BindToTableIndex(int tableIndex, const TStringBuf& path, void* opaque)
{
    auto& descriptor = GetTableDescriptorByIndex(tableIndex);

    YASSERT(descriptor.Path.empty());
    YASSERT(descriptor.Opaque == nullptr);
    descriptor.Path = path;
    descriptor.Opaque = opaque;
}

int TQueryContext::GetTableCount() const
{
    return TableDescriptors_.size();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

