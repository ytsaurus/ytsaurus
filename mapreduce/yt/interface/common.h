#pragma once

///
/// @file mapreduce/yt/interface/common.h
///
/// Header containing miscelaneous structs and classes used in library.

#include "fwd.h"

#include <statbox/ydl/runtime/cpp/traits/traits.h>

#include <library/cpp/type_info/type_info.h>
#include <library/cpp/yson/node/node.h>

#include <util/generic/guid.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/system/type_name.h>
#include <util/generic/vector.h>

#include <google/protobuf/message.h>

#include <initializer_list>
#include <type_traits>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// @cond Doxygen_Suppress

#define FLUENT_FIELD(type, name) \
    type name##_; \
    TSelf& name(const type& value) \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_FIELD_ENCAPSULATED(type, name) \
private: \
    type name##_; \
public: \
    TSelf& name(const type& value) & \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    } \
    TSelf name(const type& value) && \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    } \
    const type& name() const & \
    { \
        return name##_; \
    } \
    type name() && \
    { \
        return name##_; \
    }

#define FLUENT_FIELD_OPTION(type, name) \
    TMaybe<type> name##_; \
    TSelf& name(const type& value) \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_FIELD_OPTION_ENCAPSULATED(type, name) \
private: \
    TMaybe<type> name##_; \
public: \
    TSelf& name(const type& value) & \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    } \
    TSelf name(const type& value) && \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    } \
    TSelf& Reset##name() & \
    { \
        name##_ = Nothing(); \
        return static_cast<TSelf&>(*this); \
    } \
    TSelf Reset##name() && \
    { \
        name##_ = Nothing(); \
        return static_cast<TSelf&>(*this); \
    } \
    const TMaybe<type>& name() const& \
    { \
        return name##_; \
    } \
    TMaybe<type> name() && \
    { \
        return name##_; \
    }

#define FLUENT_FIELD_DEFAULT(type, name, defaultValue) \
    type name##_ = defaultValue; \
    TSelf& name(const type& value) \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    }

#define FLUENT_FIELD_DEFAULT_ENCAPSULATED(type, name, defaultValue) \
private: \
    type name##_ = defaultValue; \
public: \
    TSelf& name(const type& value) & \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    } \
    TSelf name(const type& value) && \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    } \
    const type& name() const & \
    { \
        return name##_; \
    } \
    type name() && \
    { \
        return name##_; \
    }

#define FLUENT_VECTOR_FIELD(type, name) \
    TVector<type> name##s_; \
    TSelf& Add##name(const type& value) \
    { \
        name##s_.push_back(value); \
        return static_cast<TSelf&>(*this);\
    } \
    TSelf& name##s(TVector<type> values) \
    { \
        name##s_ = std::move(values); \
        return static_cast<TSelf&>(*this);\
    }

#define FLUENT_VECTOR_FIELD_ENCAPSULATED(type, name) \
private: \
    TVector<type> name##s_; \
public: \
    TSelf& Add##name(const type& value) & \
    { \
        name##s_.push_back(value); \
        return static_cast<TSelf&>(*this);\
    } \
    TSelf Add##name(const type& value) && \
    { \
        name##s_.push_back(value); \
        return static_cast<TSelf&>(*this);\
    } \
    TSelf& name##s(TVector<type> value) & \
    { \
        name##s_ = std::move(value); \
        return static_cast<TSelf&>(*this);\
    } \
    TSelf name##s(TVector<type> value) && \
    { \
        name##s_ = std::move(value); \
        return static_cast<TSelf&>(*this);\
    } \
    const TVector<type>& name##s() const & \
    { \
        return name##s_; \
    } \
    TVector<type> name##s() && \
    { \
        return name##s_; \
    }

#define FLUENT_MAP_FIELD(keytype, valuetype, name) \
    TMap<keytype,valuetype> name##_; \
    TSelf& Add##name(const keytype& key, const valuetype& value) \
    { \
        name##_.emplace(key, value); \
        return static_cast<TSelf&>(*this);\
    }

/// @endcond

////////////////////////////////////////////////////////////////////////////////

template <class T, class TDerived>
struct TOneOrMany
{
    using TSelf = std::conditional_t<std::is_void_v<TDerived>, TOneOrMany, TDerived>;

    TOneOrMany()
    { }

    template<class U>
    TOneOrMany(std::initializer_list<U> il)
    {
        Parts_.assign(il.begin(), il.end());
    }

    template <class U, class... TArgs, std::enable_if_t<std::is_convertible<U, T>::value, int> = 0>
    TOneOrMany(U&& arg, TArgs&&... args)
    {
        Add(arg, std::forward<TArgs>(args)...);
    }

    TOneOrMany(TVector<T> args)
        : Parts_(std::move(args))
    { }

    bool operator==(const TOneOrMany& rhs) const {
        return Parts_ == rhs.Parts_;
    }

    template <class U, class... TArgs>
    TSelf& Add(U&& part, TArgs&&... args) &
    {
        Parts_.push_back(std::forward<U>(part));
        return Add(std::forward<TArgs>(args)...);
    }

    template <class... TArgs>
    TSelf Add(TArgs&&... args) &&
    {
        return std::move(Add(std::forward<TArgs>(args)...));
    }

    TSelf& Add() &
    {
        return static_cast<TSelf&>(*this);
    }

    TSelf Add() &&
    {
        return std::move(static_cast<TSelf&&>(*this));
    }

    TVector<T> Parts_;
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Type of the value that can occur in YT table.
///
/// @ref NYT::TTableSchema
enum EValueType : int
{
/// @cond Doxygen_Suppress
    VT_INT64,

    VT_UINT64,

    VT_DOUBLE,
    VT_BOOLEAN,

    VT_STRING,

    VT_ANY,

    VT_INT8,
    VT_INT16,
    VT_INT32,

    VT_UINT8,
    VT_UINT16,
    VT_UINT32,

    VT_UTF8,

    VT_NULL,
    VT_VOID,

    VT_DATE,
    VT_DATETIME,
    VT_TIMESTAMP,
    VT_INTERVAL,

    VT_FLOAT,
    VT_JSON,
/// @endcond
};

///
/// @brief Sort order.
///
/// @ref NYT::TTableSchema
enum ESortOrder : int
{
    /// Ascending sort order.
    SO_ASCENDING    /* "ascending" */,
    /// Descending sort order.
    SO_DESCENDING   /* "descending" */,
};

///
/// @brief Value of "optimize_for" attribute.
///
/// @ref NYT::TRichYPath
enum EOptimizeForAttr : i8
{
    OF_SCAN_ATTR    /* "scan" */,
    OF_LOOKUP_ATTR  /* "lookup" */,
};

///
/// @brief Value of "erasure_codec" attribute.
///
/// @ref NYT::TRichYPath
enum EErasureCodecAttr : i8
{
    EC_NONE_ATTR                /* "none" */,
    EC_REED_SOLOMON_6_3_ATTR    /* "reed_solomon_6_3" */,
    EC_LRC_12_2_2_ATTR          /* "lrc_12_2_2" */,
    EC_ISA_LRC_12_2_2_ATTR      /* "isa_lrc_12_2_2" */,
};

////////////////////////////////////////////////////////////////////////////////

class TSortColumn
{
public:
    using TSelf = TSortColumn;

    FLUENT_FIELD_DEFAULT_ENCAPSULATED(TString, Name, "");
    FLUENT_FIELD_DEFAULT_ENCAPSULATED(ESortOrder, SortOrder, ESortOrder::SO_ASCENDING);

    // Intentionally implicit constructors.
    TSortColumn(TStringBuf name = "", ESortOrder sortOrder = ESortOrder::SO_ASCENDING);
    TSortColumn(const TString& name, ESortOrder sortOrder = ESortOrder::SO_ASCENDING);
    TSortColumn(const char* name, ESortOrder sortOrder = ESortOrder::SO_ASCENDING);

    const TSortColumn& EnsureAscending() const;
    TNode ToNode() const;

    bool operator == (const TSortColumn& rhs) const;
    bool operator != (const TSortColumn& rhs) const;

    // The following methods are for backward compatibility
    TSortColumn& operator = (TStringBuf name);
    TSortColumn& operator = (const TString& name);
    TSortColumn& operator = (const char* name);

    bool operator == (const TStringBuf rhsName) const;
    bool operator != (const TStringBuf rhsName) const;
    bool operator == (const TString& rhsName) const;
    bool operator != (const TString& rhsName) const;
    bool operator == (const char* rhsName) const;
    bool operator != (const char* rhsName) const;

    // Intentionally implicit conversions.
    operator TString() const;
    operator TStringBuf() const;
    operator std::string() const;

    Y_SAVELOAD_DEFINE(Name_, SortOrder_);
};

class TSortColumns
    : public TOneOrMany<TSortColumn, TSortColumns>
{
public:
    using TOneOrMany<TSortColumn, TSortColumns>::TOneOrMany;

    TSortColumns();
    TSortColumns(const TVector<TString>& names);
    TSortColumns(const TColumnNames& names);

    // Intentionally implicit conversion.
    operator TColumnNames() const;

    const TSortColumns& EnsureAscending() const;
    TVector<TString> GetNames() const;
};

////////////////////////////////////////////////////////////////////////////////

NTi::TTypePtr ToTypeV3(EValueType type, bool required);

///
/// @brief Single column description
///
/// Each field describing column has setter and getter.
///
/// Example reading field:
/// ```
///    ... columnSchema.Name() ...
/// ```
///
/// Example setting field:
/// ```
///    columnSchema.Name("my-column").Type(VT_INT64); // set name and type
/// ```
class TColumnSchema
{
public:
    using TSelf = TColumnSchema;

    TColumnSchema();

    TColumnSchema(const TColumnSchema&) = default;

    FLUENT_FIELD_ENCAPSULATED(TString, Name);

    ///
    /// @brief @deprecated Functions to work with type in old manner.
    ///
    /// New code is recommended to work with types using @ref NTi::TTypePtr from type_info library.
    TColumnSchema& Type(EValueType type) &;
    TColumnSchema Type(EValueType type) &&;
    EValueType Type() const;

    /// @brief Set and get column type.
    /// @{
    TColumnSchema& Type(const NTi::TTypePtr& type) &;
    TColumnSchema Type(const NTi::TTypePtr& type) &&;

    TColumnSchema& TypeV3(const NTi::TTypePtr& type) &;
    TColumnSchema TypeV3(const NTi::TTypePtr& type) &&;
    NTi::TTypePtr TypeV3() const;
    /// @}

    // Experimental features
    FLUENT_FIELD_OPTION_ENCAPSULATED(TNode, RawTypeV2);
    FLUENT_FIELD_OPTION_ENCAPSULATED(TNode, RawTypeV3);

    FLUENT_FIELD_OPTION_ENCAPSULATED(ESortOrder, SortOrder);
    FLUENT_FIELD_OPTION_ENCAPSULATED(TString, Lock);
    FLUENT_FIELD_OPTION_ENCAPSULATED(TString, Expression);
    FLUENT_FIELD_OPTION_ENCAPSULATED(TString, Aggregate);
    FLUENT_FIELD_OPTION_ENCAPSULATED(TString, Group);

    bool Required() const;
    TColumnSchema& Type(EValueType type, bool required) &;
    TColumnSchema Type(EValueType type, bool required) &&;

private:
    friend void Deserialize(TColumnSchema& columnSchema, const TNode& node);
    NTi::TTypePtr TypeV3_;
    bool Required_ = false;
};

bool operator==(const TColumnSchema& lhs, const TColumnSchema& rhs);
bool operator!=(const TColumnSchema& lhs, const TColumnSchema& rhs);

class TTableSchema
{
public:
    using TSelf = TTableSchema;

    FLUENT_VECTOR_FIELD_ENCAPSULATED(TColumnSchema, Column);
    FLUENT_FIELD_DEFAULT_ENCAPSULATED(bool, Strict, true);
    FLUENT_FIELD_DEFAULT_ENCAPSULATED(bool, UniqueKeys, false);

    TVector<TColumnSchema>& MutableColumns();
    bool Empty() const;

public:
    // Some helper methods

    TTableSchema& AddColumn(const TString& name, EValueType type) &;
    TTableSchema AddColumn(const TString& name, EValueType type) &&;

    TTableSchema& AddColumn(const TString& name, EValueType type, ESortOrder sortOrder) &;
    TTableSchema AddColumn(const TString& name, EValueType type, ESortOrder sortOrder) &&;

    TTableSchema& AddColumn(const TString& name, const NTi::TTypePtr& type) &;
    TTableSchema AddColumn(const TString& name, const NTi::TTypePtr& type) &&;

    TTableSchema& AddColumn(const TString& name, const NTi::TTypePtr& type, ESortOrder sortOrder) &;
    TTableSchema AddColumn(const TString& name, const NTi::TTypePtr& type, ESortOrder sortOrder) &&;

    TTableSchema& SortBy(const TSortColumns& columns) &;
    TTableSchema SortBy(const TSortColumns& columns) &&;

    TNode ToNode() const;

    friend void Deserialize(TTableSchema& tableSchema, const TNode& node);
};

bool operator==(const TTableSchema& lhs, const TTableSchema& rhs);
bool operator!=(const TTableSchema& lhs, const TTableSchema& rhs);

TTableSchema CreateTableSchema(
    const ::google::protobuf::Descriptor& messageDescriptor,
    const TSortColumns& sortColumns = TSortColumns(),
    bool keepFieldsWithoutExtension = true);

template <class TProtoType, typename = std::enable_if_t<std::is_base_of_v<::google::protobuf::Message, TProtoType>>>
inline TTableSchema CreateTableSchema(
    const TSortColumns& sortColumns = TSortColumns(),
    bool keepFieldsWithoutExtension = true)
{
    static_assert(
        std::is_base_of_v<::google::protobuf::Message, TProtoType>,
        "Template argument must be derived from ::google::protobuf::Message");

    return CreateTableSchema(
        *TProtoType::descriptor(),
        sortColumns,
        keepFieldsWithoutExtension);
}

TTableSchema CreateTableSchema(NTi::TTypePtr type);

template <class TYdlType, typename = std::enable_if_t<NYdl::TIsYdlGenerated<TYdlType>::value>>
inline TTableSchema CreateTableSchema()
{
    static_assert(
        NYdl::TIsYdlGenerated<TYdlType>::value,
        "Template argument must be YDL generated type");

    return CreateTableSchema(NYdl::TYdlTraits<TYdlType>::Reflect());
}

////////////////////////////////////////////////////////////////////////////////

enum class ERelation
{
    Less            /* "<"  */,
    LessOrEqual     /* "<=" */,
    Greater         /* ">"  */,
    GreaterOrEqual  /* ">=" */,
};

struct TKeyBound
{
    using TSelf = TKeyBound;

    explicit TKeyBound(ERelation relation = ERelation::Less, TKey key = TKey{});

    FLUENT_FIELD_DEFAULT_ENCAPSULATED(ERelation, Relation, ERelation::Less);
    FLUENT_FIELD_DEFAULT_ENCAPSULATED(TKey, Key, TKey{});
};

struct TReadLimit
{
    using TSelf = TReadLimit;

    FLUENT_FIELD_OPTION(TKeyBound, KeyBound);
    FLUENT_FIELD_OPTION(TKey, Key);
    FLUENT_FIELD_OPTION(i64, RowIndex);
    FLUENT_FIELD_OPTION(i64, Offset);
    FLUENT_FIELD_OPTION(i64, TabletIndex);
};

struct TReadRange
{
    using TSelf = TReadRange;

    FLUENT_FIELD(TReadLimit, LowerLimit);
    FLUENT_FIELD(TReadLimit, UpperLimit);
    FLUENT_FIELD(TReadLimit, Exact);

    static TReadRange FromRowIndices(i64 lowerLimit, i64 upperLimit)
    {
        return TReadRange()
            .LowerLimit(TReadLimit().RowIndex(lowerLimit))
            .UpperLimit(TReadLimit().RowIndex(upperLimit));
    }

    static TReadRange FromKeys(const TKey& lowerKeyInclusive, const TKey& upperKeyExclusive)
    {
        return TReadRange()
            .LowerLimit(TReadLimit().Key(lowerKeyInclusive))
            .UpperLimit(TReadLimit().Key(upperKeyExclusive));
    }
};

struct TRichYPath
{
    using TSelf = TRichYPath;

    FLUENT_FIELD(TYPath, Path);

    FLUENT_FIELD_OPTION(bool, Append);
    FLUENT_FIELD_OPTION(bool, PartiallySorted);
    FLUENT_FIELD(TSortColumns, SortedBy);

    FLUENT_VECTOR_FIELD(TReadRange, Range);

    // Specifies columns that should be read.
    // If it's set to Nothing then all columns will be read.
    // If empty TColumnNames is specified then each read row will be empty.
    FLUENT_FIELD_OPTION(TColumnNames, Columns);

    FLUENT_FIELD_OPTION(bool, Teleport);
    FLUENT_FIELD_OPTION(bool, Primary);
    FLUENT_FIELD_OPTION(bool, Foreign);
    FLUENT_FIELD_OPTION(i64, RowCountLimit);

    FLUENT_FIELD_OPTION(TString, FileName);
    FLUENT_FIELD_OPTION(TYPath, OriginalPath);
    FLUENT_FIELD_OPTION(bool, Executable);
    FLUENT_FIELD_OPTION(TNode, Format);
    FLUENT_FIELD_OPTION(TTableSchema, Schema);

    FLUENT_FIELD_OPTION(TString, CompressionCodec);
    FLUENT_FIELD_OPTION(EErasureCodecAttr, ErasureCodec);
    FLUENT_FIELD_OPTION(EOptimizeForAttr, OptimizeFor);

    // @brief This attribute is used when specifying the files required for an operation.
    //
    // If BypassArtifactCache == true, file will be loaded into the job's sandbox bypassing the cache on the YT node.
    // It helps jobs that use tmpfs to start faster,
    // because files will be loaded into tmpfs directly bypassing disk cache
    FLUENT_FIELD_OPTION(bool, BypassArtifactCache);

    // Timestamp of dynamic table.
    // NOTE: it is _not_ unix timestamp
    // (instead it's transaction timestamp, that is more complex structure).
    FLUENT_FIELD_OPTION(i64, Timestamp);

    // Specifiy transaction that should be used to access this path.
    // Allows to start cross-transactional operations.
    FLUENT_FIELD_OPTION(TTransactionId, TransactionId);

    // Specifies columnar mapping which will be applied to columns before transfer to job.
    using TRenameColumnsDescriptor = THashMap<TString, TString>;
    FLUENT_FIELD_OPTION(TRenameColumnsDescriptor, RenameColumns);

    TRichYPath()
    { }

    TRichYPath(const char* path)
        : Path_(path)
    { }

    TRichYPath(const TYPath& path)
        : Path_(path)
    { }
};

template <typename TProtoType>
TRichYPath WithSchema(const TRichYPath& path, const TSortColumns& sortBy = TSortColumns())
{
    auto schemedPath = path;
    if (!schemedPath.Schema_) {
        schemedPath.Schema(CreateTableSchema<TProtoType>(sortBy));
    }
    return schemedPath;
}

template <typename TRowType>
TRichYPath MaybeWithSchema(const TRichYPath& path, const TSortColumns& sortBy = TSortColumns())
{
    if constexpr (std::is_base_of_v<::google::protobuf::Message, TRowType>) {
        return WithSchema<TRowType>(path, sortBy);
    } else {
        return path;
    }
}

////////////////////////////////////////////////////////////////////////////////

// Statistics about table columns.
struct TTableColumnarStatistics
{
    // Total data weight for all chunks for each of requested columns.
    THashMap<TString, i64> ColumnDataWeight;

    // Total weight of all old chunks that don't keep columnar statitics.
    i64 LegacyChunksDataWeight = 0;

    // Timestamps total weight (only for dynamic tables).
    TMaybe<i64> TimestampTotalWeight;
};

////////////////////////////////////////////////////////////////////////////////

/// @brief Contains information about tablet
/// This struct is returned by @ref NYT::IClient::GetTabletInfos
struct TTabletInfo
{
    /// @brief Indicates the total number of rows added to the tablet (including trimmed ones).
    /// Currently only provided for ordered tablets.
    i64 TotalRowCount = 0;

    /// @brief Contains the number of front rows that are trimmed and are not guaranteed to be accessible.
    /// Only makes sense for ordered tablet.
    i64 TrimmedRowCount = 0;

    /// @brief Contains the barrier timestamp of the tablet cell containing the tablet, which lags behind the current timestamp.
    /// It is guaranteed that all transactions with commit timestamp not exceeding the barrier
    /// are fully committed; e.g. all their addes rows are visible (and are included in TTabletInfo::TotalRowCount).
    /// Mostly makes sense for ordered tablets.
    ui64 BarrierTimestamp;
};

////////////////////////////////////////////////////////////////////////////////

struct TAttributeFilter
{
    using TSelf = TAttributeFilter;

    FLUENT_VECTOR_FIELD(TString, Attribute);
};

////////////////////////////////////////////////////////////////////////////////

bool IsTrivial(const TReadLimit& readLimit);

EValueType NodeTypeToValueType(TNode::EType nodeType);

////////////////////////////////////////////////////////////////////////////////

enum class EMasterReadKind : int
{
    Leader /* "leader" */,
    Follower /* "follower" */,
    Cache /* "cache" */,
    MasterCache /* "master_cache" */,
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

// MUST NOT BE USED BY CLIENTS
// TODO: we should use default GENERATE_ENUM_SERIALIZATION
TString ToString(EValueType type);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
