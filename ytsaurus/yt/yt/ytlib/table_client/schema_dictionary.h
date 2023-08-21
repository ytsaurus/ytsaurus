#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TSchemaDictionary
{
public:
    int GetIdOrRegisterTable(const TTableSchema& table);
    int GetIdOrRegisterColumn(const TColumnSchema& column);
    const TTableSchemaPtr& GetTable(int id) const;
    const TColumnSchema& GetColumn(int id) const;

private:
    //! Mapping from known column schemas to ids and reverse.
    THashMap<TColumnSchema, int> ColumnToId_;
    std::vector<TColumnSchema> IdToColumn_;

    //! Separate id space for deleted columns.
    THashMap<TDeletedColumn, int> DeletedColumnToId_;
    std::vector<TDeletedColumn> IdToDeletedColumn_;

    using TTableSchemaInternal = NProto::TSchemaDictionary::TTableSchemaInternal;

    //! Hasher for proto class TTableSchemaInternal.
    struct THashInternal
    {
        inline size_t operator() (const TTableSchemaInternal& tableSchema) const;
    };

    //! Equality predicate for proto class TTableSchemaInternal.
    struct TEqualsInternal
    {
        inline bool operator() (const TTableSchemaInternal& lhs, const TTableSchemaInternal& rhs) const;
    };

    //! Mapping from known table schemas to ids and two its reverses (to the internal
    //! table schema representation and to the "real" table schema).
    THashMap<TTableSchemaInternal, int, THashInternal, TEqualsInternal> TableInternalToId_;
    std::vector<TTableSchemaInternal> IdToTableInternal_;
    std::vector<TTableSchemaPtr> IdToTable_;

    int GetIdOrRegisterDeletedColumn(const TDeletedColumn& deletedColumn);

    friend void ToProto(NProto::TSchemaDictionary* protoDictionary, const TSchemaDictionary& dictionary);
    friend void FromProto(TSchemaDictionary* dictionary, const NProto::TSchemaDictionary& protoDictionary);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NY::NTableClient
