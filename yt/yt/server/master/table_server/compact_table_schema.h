#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <library/cpp/yt/threading/atomic_object.h>
#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NTableServer {

///////////////////////////////////////////////////////////////////////////////

//! TCompactTableSchema is a light version of NTableClient::TTableSchema.
//! It stores minimum lightweighted and most-used fields of TTableSchema and compactified wire protobuf representation of schema itself.
class TCompactTableSchema final
{
public:
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(Empty, true);
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(Strict, false);
    DEFINE_BYVAL_RO_BOOLEAN_PROPERTY(UniqueKeys, false);

    static NThreading::TAtomicObject<TDuration> CacheExpirationTimeout;

public:
    //! Constructs an empty non-strict schema.
    TCompactTableSchema();

    explicit TCompactTableSchema(const NTableClient::NProto::TTableSchemaExt& schema);
    explicit TCompactTableSchema(const NTableClient::TTableSchema& schema);

    bool operator==(const TCompactTableSchema& other) const = default;

    const NTableClient::TTableSchema& AsHeavyTableSchema() const;
    const std::string& AsWireProto() const;

    bool IsSorted() const;

    bool HasHunkColumns() const;
    bool HasNontrivialSchemaModification() const;

    int GetKeyColumnCount() const;
    const NTableClient::TKeyColumns& GetKeyColumns() const;
    const std::vector<NTableClient::ESortOrder>& GetSortOrders() const;

    i64 GetMemoryUsage() const;

    NTableClient::TComparator ToComparator(TCallback<NTableClient::TUUComparerSignature> cgComparator = {}) const;
    TCompactTableSchemaPtr ToModifiedSchema(NTableClient::ETableSchemaModification modification) const;
    TCompactTableSchemaPtr ToUniqueKeys() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

private:
    std::string TableSchema_;

    // Should have no impact on comparison of TCompactTableSchema.
    struct TCachedTableSchema
    {
        TCachedTableSchema() = default;
        TCachedTableSchema(const TCachedTableSchema& other);
        TCachedTableSchema operator=(const TCachedTableSchema& other);
        bool operator==(const TCachedTableSchema& other);

        NTableClient::TTableSchemaPtr TableSchema;
        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, TableSchemaLock);
    };

    mutable TCachedTableSchema Cache_;

    bool HasHunkColumns_ = false;
    bool HasNontrivialSchemaModification_ = false;
    NTableClient::TKeyColumns KeyColumns_;
    std::vector<NTableClient::ESortOrder> SortOrders_;

    void InitializePartial(
        bool empty,
        bool strict,
        bool uniqueKeys,
        NTableClient::ETableSchemaModification schemaModification);

    void InitializeFromProto(const NTableClient::NProto::TTableSchemaExt& protoSchema);

    void DeserializeToProto(NTableClient::NProto::TTableSchemaExt* protoSchema) const;

    friend void ToProto(NTableClient::NProto::TTableSchemaExt* protoSchema, const TCompactTableSchema& schema);
};

DEFINE_REFCOUNTED_TYPE(TCompactTableSchema)

////////////////////////////////////////////////////////////////////////////////

void ToProto(NTableClient::NProto::TTableSchemaExt* protoSchema, const TCompactTableSchema& schema);
void ToProto(NTableClient::NProto::TTableSchemaExt* protoSchema, const TCompactTableSchemaPtr& schema);
void FromProto(TCompactTableSchema* schema, const NTableClient::NProto::TTableSchemaExt& protoSchema);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TCompactTableSchema& schema, TStringBuf spec);
void FormatValue(TStringBuilderBase* builder, const TCompactTableSchemaPtr& schema, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct TCompactTableSchemaHash
{
    size_t operator()(const TCompactTableSchema& schema) const;
    size_t operator()(const TCompactTableSchemaPtr& schema) const;
};

struct TCompactTableSchemaEquals
{
    bool operator()(const TCompactTableSchema& lhs, const TCompactTableSchema& rhs) const;
    bool operator()(const TCompactTableSchemaPtr& lhs, const TCompactTableSchemaPtr& rhs) const;
    bool operator()(const TCompactTableSchemaPtr& lhs, const TCompactTableSchema& rhs) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableServer

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NTableServer::TCompactTableSchema>
{
    size_t operator()(const NYT::NTableServer::TCompactTableSchema& tableSchema) const;
};

////////////////////////////////////////////////////////////////////////////////
