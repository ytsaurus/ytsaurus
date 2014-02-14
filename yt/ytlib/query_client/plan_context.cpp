#include "plan_context.h"

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NQueryClient {

using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

static const size_t InitialMemoryPoolSize = 4096;
static const int FakeTableIndex = 0xdeadbabe;

////////////////////////////////////////////////////////////////////////////////

TPlanContext::TTrackedObject::TTrackedObject(TPlanContext* context)
    : Context_(context)
{
    Context_->TrackedObjects_.insert(this);
}

TPlanContext::TTrackedObject::~TTrackedObject()
{
    Context_ = nullptr;
}

void* TPlanContext::TTrackedObject::operator new(
    size_t bytes,
    TPlanContext* context)
{
    return context->Allocate(bytes);
}

void TPlanContext::TTrackedObject::operator delete(
    void* pointer,
    TPlanContext* context) throw()
{
    context->Deallocate(pointer);
}

void* TPlanContext::TTrackedObject::operator new(size_t)
{
    YUNREACHABLE();
}

void TPlanContext::TTrackedObject::operator delete(void*) throw()
{
    YUNREACHABLE();
}

////////////////////////////////////////////////////////////////////////////////

TPlanContext::TPlanContext(TTimestamp timestamp)
    : Timestamp_(timestamp)
    , MemoryPool_(InitialMemoryPoolSize)
    , NodeDirectory_(New<TNodeDirectory>())
{ }

TPlanContext::~TPlanContext()
{
    for (auto& object : TrackedObjects_) {
        object->~TTrackedObject();
        TTrackedObject::operator delete(object, this);
    }
}

void* TPlanContext::Allocate(size_t size)
{
    return MemoryPool_.Allocate(size);
}

void TPlanContext::Deallocate(void*)
{ }

TStringBuf TPlanContext::Capture(const char* begin, const char* end)
{
    return TStringBuf(MemoryPool_.Append(begin, end - begin), end - begin);
}

void TPlanContext::SetDebugInformation(TDebugInformation&& debugInformation)
{
    DebugInformation_ = std::move(debugInformation);
}

const TDebugInformation* TPlanContext::GetDebugInformation() const
{
    return DebugInformation_.GetPtr();
}

TTableDescriptor& TPlanContext::TableDescriptor()
{
    return TableDescriptor_;
}

TNodeDirectoryPtr TPlanContext::GetNodeDirectory() const
{
    return NodeDirectory_;
}

TTimestamp TPlanContext::GetTimestamp() const
{
    return Timestamp_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

