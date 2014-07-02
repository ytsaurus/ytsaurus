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

        TPlanContext* GetAssociatedContext();

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

    explicit TPlanContext(
        TTimestamp timestamp = NullTimestamp,
        i64 inputRowLimit = std::numeric_limits<i64>::max(),
        i64 outputRowLimit = std::numeric_limits<i64>::max());
    ~TPlanContext();

    void* Allocate(size_t size);
    void Deallocate(void* pointer);
    TStringBuf Capture(const char* begin, const char* end);
    TStringBuf Capture(const TStringBuf& stringBuf);


    DEFINE_BYVAL_RW_PROPERTY(Stroka, Source);
    DEFINE_BYVAL_RW_PROPERTY(Stroka, TablePath);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, Timestamp);
    DEFINE_BYVAL_RW_PROPERTY(i64, InputRowLimit);
    DEFINE_BYVAL_RW_PROPERTY(i64, OutputRowLimit);

public:
    NNodeTrackerClient::TNodeDirectoryPtr GetNodeDirectory() const;

    template <class TType, class... TArgs>
    TType* TrackedNew(TArgs&&... args)
    {
        return new (this) TType(this, std::forward<TArgs>(args)...);
    }

private:
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;

    TChunkedMemoryPool MemoryPool_;
    std::unordered_set<TTrackedObject*> TrackedObjects_;

};

DEFINE_REFCOUNTED_TYPE(TPlanContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

