#pragma once

#include "public.h"

#include <core/misc/nullable.h>
#include <core/misc/small_vector.h>
#include <core/misc/chunked_memory_pool.h>

#include <ytlib/node_tracker_client/public.h>
#include <ytlib/transaction_client/public.h>

#include <unordered_set>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

//! Holds useful information about (input) table.
struct TTableDescriptor
{
    Stroka Alias;
    Stroka Path;
    void* Opaque;
};

//! Holds query plan nodes and related stuff.
class TPlanContext
    : public TRefCounted
{
public:
    class TTrackedObject
    {
    public:
        explicit TTrackedObject(TPlanContext* context);
        virtual ~TTrackedObject();

        // Bound objects have to be allocated through the context.
        void* operator new(size_t size, TPlanContext* context);
        void operator delete(void* pointer, TPlanContext* context) throw();

    protected:
        TPlanContext* Context_;

        // Bound objects could not be instantiated without the context.
        TTrackedObject() = delete;
        TTrackedObject(const TTrackedObject&) = delete;
        TTrackedObject(TTrackedObject&&) = delete;
        TTrackedObject& operator=(const TTrackedObject&) = delete;
        TTrackedObject& operator=(TTrackedObject&&) = delete;

        // Bound objects could not be allocated nor freed with regular operators.
        void* operator new(size_t);
        void operator delete(void*) throw();

    };

    explicit TPlanContext(TTimestamp timestamp = NullTimestamp);
    ~TPlanContext();

    void* Allocate(size_t size);
    void Deallocate(void* pointer);
    TStringBuf Capture(const char* begin, const char* end);

    void SetSource(Stroka source);
    Stroka GetSource() const;

    void SetTablePath(Stroka tablePath);
    Stroka GetTablePath() const;

    void SetTimestamp(TTimestamp timestamp);
    TTimestamp GetTimestamp() const;

    NNodeTrackerClient::TNodeDirectoryPtr GetNodeDirectory() const;

    template <class TType, class... TArgs>
    TType* TrackedNew(TArgs&&... args)
    {
        return new (this) TType(this, std::forward<TArgs>(args)...);
    }

private:
    Stroka Source_;
    Stroka TablePath_;
    TTimestamp Timestamp_;

    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;

    TChunkedMemoryPool MemoryPool_;
    std::unordered_set<TTrackedObject*> TrackedObjects_;

};

DEFINE_REFCOUNTED_TYPE(TPlanContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

