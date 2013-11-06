#pragma once

#include "public.h"

#include <core/misc/nullable.h>
#include <core/misc/small_vector.h>

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
        void* operator new(size_t, TPlanContext*);
        void operator delete(void*, TPlanContext*) throw();

    protected:
        TPlanContext* Context_;

    protected:
        // Bound objects could not be instatiated without the context.
        TTrackedObject();
        TTrackedObject(const TTrackedObject&);
        TTrackedObject(TTrackedObject&&);
        TTrackedObject& operator=(const TTrackedObject&);
        TTrackedObject& operator=(TTrackedObject&&);

        // Bound objects could not be allocated nor freed with regular operators.
        void* operator new(size_t);
        void operator delete(void*) throw();
    };

    TPlanContext();
    ~TPlanContext();

    void* Allocate(size_t size);
    void Deallocate(void* pointer);
    TStringBuf Capture(const char* begin, const char* end);

    void SetDebugInformation(TDebugInformation&& debugInformation);
    const TDebugInformation* GetDebugInformation() const;

    int GetTableIndexByAlias(const TStringBuf& alias);
    int GetFakeTableIndex();
    TTableDescriptor& GetTableDescriptorByIndex(int tableIndex);
    void BindToTableIndex(int tableIndex, const TStringBuf& path, void* opaque);
    int GetTableCount() const;

private:
    TMemoryPool MemoryPool_;
    TNullable<TDebugInformation> DebugInformation_;

    std::unordered_set<TTrackedObject*> TrackedObjects_;
    std::vector<TTableDescriptor> TableDescriptors_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

