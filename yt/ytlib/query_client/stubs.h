#pragma once

#include "public.h"

#include <core/misc/common.h>
#include <core/misc/error.h>
#include <core/misc/nullable.h>

#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/object_client/public.h>

#include <ytlib/ypath/public.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////
// In use.

typedef NYT::NChunkClient::NProto::TChunkSpec TDataSplit;

static const int TypicalTableCount = 2;

DECLARE_ENUM(EColumnType,
    (TheBottom)
    (Integer)
    (Double)
    (String)
    (Any)
    (Null)
);

struct TColumnSchema
{
    Stroka Name;
    EColumnType Type;
};

struct TTableSchema
{
    std::vector<TColumnSchema> Columns;
};

struct TTableInfo
{
    NYT::NObjectClient::TObjectId Id;
    std::vector<Stroka> KeyColumns;
    TTableSchema Schema;
};

struct IPreparationHooks
{
    virtual ~IPreparationHooks()
    { }

    virtual TFuture<TErrorOr<TDataSplit>> GetInitialSplit(const NYT::NYPath::TYPath& path) = 0;
};

////////////////////////////////////////////////////////////////////////////////
// Others.

struct TRowValue
{
    typedef union {
        i64 Integer;
        double Double;
        const char* String;
        void* Any;
    } TUnionType;

    typedef typename std::aligned_storage<sizeof(TUnionType), alignof(TUnionType)>::type 
        TStorageType;

    TStorageType Data; // Holds the value.
    ui32 Length; // For variable-sized values.
    ui16 Type; // EColumnType
    ui16 Index; // TNameTable
};

static_assert(sizeof(TRowValue) == 16, "TRowValue has to be exactly 16 bytes");

struct TRow
{
    void* Opaque;

    const TRowValue& operator[](int index) const;
};

static_assert(sizeof(TRow) == 8, "TRow has to be exactly 8 bytes");

class TNameTable
    : public TRefCounted
{
    i16 GetIndex(const TStringBuf& name);
    TNullable<i16> FindIndex(const TStringBuf& name);

    const TStringBuf& GetName(i16);

    i16 Internalize(const TStringBuf& name);
};

typedef TIntrusivePtr<TNameTable> TNameTablePtr;

struct IMegaReader
{
    TAsyncError Open(TNameTablePtr nameTable, const TTableSchema& schema, bool includeAllColumns);
    bool Read(std::vector<TRow>* rows);
    TAsyncError GetReadyEvent();
};

typedef TIntrusivePtr<IMegaReader> IMegaReaderPtr;

struct IMegaWriter
{
    TAsyncError Open(TNameTablePtr nameTable);

    bool WriteValue(const TRowValue* value);
    bool WriteRow();

    TAsyncError GetReadyEvent();
};

typedef TIntrusivePtr<IMegaWriter> IMegaWriterPtr;

////////////////////////////////////////////////////////////////////////////////
// Hooks for other means.

struct IQueryNode
{
    virtual ~IQueryNode()
    { }

    virtual IMegaReaderPtr Execute(const TQueryFragment& fragment) = 0;
};

typedef TIntrusivePtr<IQueryNode> IQueryNodePtr;

struct ICoordinationHooks
{
    virtual ~ICoordinationHooks()
    { }

    virtual std::vector<TDataSplit> SplitFurther(const TDataSplit& split) = 0;
    virtual IQueryNodePtr GetCollocatedExecutor(const TDataSplit& split) = 0;
    virtual IQueryNodePtr GetLocalExecutor() = 0;
};

struct IExecutionHooks
{
    virtual ~IExecutionHooks()
    { }

    virtual IMegaReaderPtr GetReader(const TDataSplit& split) = 0;
    virtual IMegaWriterPtr GetWriter() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

