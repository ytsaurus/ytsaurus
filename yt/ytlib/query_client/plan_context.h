#pragma once

#include "public.h"

#include <core/misc/nullable.h>
#include <core/misc/small_vector.h>

#include <ytlib/node_tracker_client/public.h>

#include <util/memory/pool.h>

#include <unordered_set>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

//! Holds useful debug information.
//! Usually this structure is not preserved between (de)serializations.
struct TDebugInformation
{
    TDebugInformation(const Stroka& source)
        : Source(source)
    { }

    Stroka Source;
};

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
        TTrackedObject();
        TTrackedObject(const TTrackedObject&);
        TTrackedObject(TTrackedObject&&);
        TTrackedObject& operator=(const TTrackedObject&);
        TTrackedObject& operator=(TTrackedObject&&);

        // Bound objects could not be allocated nor freed with regular operators.
        void* operator new(size_t);
        void operator delete(void*) throw();

    };

    explicit TPlanContext(TTimestamp timestamp = NullTimestamp);
    ~TPlanContext();

    void* Allocate(size_t size);
    void Deallocate(void* pointer);
    TStringBuf Capture(const char* begin, const char* end);

    void SetDebugInformation(TDebugInformation&& debugInformation);
    const TDebugInformation* GetDebugInformation() const;

    TTableDescriptor& TableDescriptor();

    NNodeTrackerClient::TNodeDirectoryPtr GetNodeDirectory() const;

    TTimestamp GetTimestamp() const;

private:
    TTimestamp Timestamp_;

    TMemoryPool MemoryPool_;
    TNullable<TDebugInformation> DebugInformation_;

    std::unordered_set<TTrackedObject*> TrackedObjects_;
    TTableDescriptor TableDescriptor_;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;

};

DEFINE_REFCOUNTED_TYPE(TPlanContext)

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

