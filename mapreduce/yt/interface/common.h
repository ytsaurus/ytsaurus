#pragma once

#include <util/generic/guid.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>

#include <util/generic/type_name.h>

#include "node.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

using TTransactionId = TGUID;
using TNodeId = TGUID;
using TLockId = TGUID;
using TOperationId = TGUID;

using TYPath = Stroka;
using TLocalFilePath = Stroka;

////////////////////////////////////////////////////////////////////////////////

#define FLUENT_FIELD(type, name) \
    type name##_; \
    TSelf& name(const type& value) \
    { \
        name##_ = value; \
        return *this; \
    }

#define FLUENT_FIELD_OPTION(type, name) \
    TMaybe<type> name##_; \
    TSelf& name(const type& value) \
    { \
        name##_ = value; \
        return *this; \
    }

#define FLUENT_FIELD_DEFAULT(type, name, defaultValue) \
    type name##_ = defaultValue; \
    TSelf& name(const type& value) \
    { \
        name##_ = value; \
        return *this; \
    }

#define FLUENT_VECTOR_FIELD(type, name) \
    yvector<type> name##s_; \
    TSelf& Add##name(const type& value) \
    { \
        name##s_.push_back(value); \
        return *this;\
    }

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TKeyBase
{
    TKeyBase()
    { }

    TKeyBase(const TKeyBase& rhs)
    {
        Parts_ = rhs.Parts_;
    }

    TKeyBase& operator=(const TKeyBase& rhs)
    {
        Parts_ = rhs.Parts_;
        return *this;
    }

    TKeyBase(TKeyBase&& rhs)
    {
        Parts_ = MoveArg(rhs.Parts_);
    }

    TKeyBase& operator=(TKeyBase&& rhs)
    {
        Parts_ = MoveArg(rhs.Parts_);
        return *this;
    }

    template <class... TArgs>
    TKeyBase(TArgs&&... args)
    {
        Add(ForwardArg<TArgs>(args)...);
    }

    template <class U, class... TArgs>
    void Add(U&& part, TArgs&&... args)
    {
        Parts_.push_back(ForwardArg<U>(part));
        Add(ForwardArg<TArgs>(args)...);
    }

    void Add()
    { }

    yvector<T> Parts_;
};

// key column values
using TKey = TKeyBase<TNode>;

// key column names
using TKeyColumns = TKeyBase<Stroka>;

////////////////////////////////////////////////////////////////////////////////

struct TReadLimit
{
    using TSelf = TReadLimit;

    FLUENT_FIELD_OPTION(TKey, Key);
    FLUENT_FIELD_OPTION(i64, RowIndex);
    FLUENT_FIELD_OPTION(i64, Offset);
};

struct TReadRange
{
    using TSelf = TReadRange;

    FLUENT_FIELD(TReadLimit, LowerLimit);
    FLUENT_FIELD(TReadLimit, UpperLimit);

    static TReadRange FromRowIndexes(i64 lowerLimit, i64 upperLimit)
    {
        return TReadRange()
            .LowerLimit(TReadLimit().RowIndex(lowerLimit))
            .UpperLimit(TReadLimit().RowIndex(upperLimit));
    }
};

struct TRichYPath
{
    using TSelf = TRichYPath;

    FLUENT_FIELD(TYPath, Path);
    FLUENT_FIELD_DEFAULT(bool, Append, false);
    FLUENT_VECTOR_FIELD(TReadRange, Range);
    FLUENT_FIELD(TKeyColumns, Columns);
    FLUENT_FIELD(TKeyColumns, SortedBy);

    TRichYPath()
    { }

    TRichYPath(const char* path, bool append = false)
        : Path_(path)
        , Append_(append)
    { }

    TRichYPath(const TYPath& path, bool append = false)
        : Path_(path)
        , Append_(append)
    { }
};

struct TAttributeFilter
{
    using TSelf = TAttributeFilter;

    FLUENT_VECTOR_FIELD(Stroka, Attribute);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
