#include "stdafx.h"
#include "plan_context.h"

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NQueryClient {

using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

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

struct TPlanContextPoolTag { };

TPlanContext::TPlanContext(TTimestamp timestamp)
    : Timestamp_(timestamp)
    , NodeDirectory_(New<TNodeDirectory>())
    , MemoryPool_(GetRefCountedTrackerCookie<TPlanContextPoolTag>())
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
    size_t length = end - begin;
    char* buffer = MemoryPool_.Allocate(length);

    ::memcpy(buffer, begin, length);

    return TStringBuf(buffer, length);
}

void TPlanContext::SetSource(Stroka source) {
    Source_ = std::move(source);
}

Stroka TPlanContext::GetSource() const {
    return Source_;
}

void TPlanContext::SetTablePath(Stroka tablePath) {
    TablePath_ = std::move(tablePath);
}

Stroka TPlanContext::GetTablePath() const {
    return TablePath_;
}

void TPlanContext::SetTimestamp(TTimestamp timestamp) {
    Timestamp_ = std::move(timestamp);
}

TTimestamp TPlanContext::GetTimestamp() const
{
    return Timestamp_;
}

TNodeDirectoryPtr TPlanContext::GetNodeDirectory() const
{
    return NodeDirectory_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

