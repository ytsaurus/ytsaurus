#pragma once

#include "public.h"
#include "stubs.h"

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

//! Holds query context and ASTs.
class TQueryContext
    : public TRefCounted
{
public:
    class TTrackedObject
    {
    public:
        explicit TTrackedObject(TQueryContext* context);
        virtual ~TTrackedObject();

        // Bound objects have to be allocated through the context.
        void* operator new(size_t, TQueryContext*);
        void operator delete(void*, TQueryContext*) throw();

    protected:
        TQueryContext* Context_;

        // Bound objects could not be allocated nor freed with regular operators.
        void* operator new(size_t);
        void operator delete(void*) throw();
    };

    TQueryContext();
    ~TQueryContext();

    void* Allocate(size_t size);
    void Deallocate(void* pointer);
    TStringBuf Capture(const char* begin, const char* end);

    void SetDebugInformation(TDebugInformation&& debugInformation);
    const TDebugInformation* GetDebugInformation() const;

    int GetTableIndexByAlias(const TStringBuf& alias);
    TTableDescriptor& GetTableDescriptorByIndex(int tableIndex);
    void BindToTableIndex(int tableIndex, const TStringBuf& path, void* opaque);

private:
    TMemoryPool MemoryPool_;
    std::unordered_set<TTrackedObject*> TrackedObjects_;

    TNullable<TDebugInformation> DebugInformation_;
    TSmallVector<TTableDescriptor, TypicalTableCount> TableDescriptors_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

