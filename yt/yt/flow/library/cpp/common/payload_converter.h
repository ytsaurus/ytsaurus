#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/misc/compact_unversioned_owning_row.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/library/query/engine_api/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Converts a row from one schema to another.
//! Instances are immutable and thread-safe.
struct IPayloadConverter
    : public TRefCounted
{
    //! Converts a row. Source schema must match the schema this converter was built for.
    virtual TCompactUnversionedOwningRow Convert(const TCompactUnversionedOwningRow& row) const = 0;

    //! Convenience overloads — same conversion, with strong typedefs preserved.
    TPayload Convert(const TPayload& payload) const
    {
        return TPayload(Convert(payload.Underlying()));
    }

    TKey Convert(const TKey& key) const
    {
        return TKey(Convert(key.Underlying()));
    }
};

DEFINE_REFCOUNTED_TYPE(IPayloadConverter)

////////////////////////////////////////////////////////////////////////////////

//! Cache of IPayloadConverter instances keyed by (sourceSchema, targetSchema) pair.
struct IPayloadConverterCache
    : public TRefCounted
{
    //! Returns a converter for the given schema pair, creating one if not cached.
    virtual IPayloadConverterPtr Get(
        const NTableClient::TTableSchemaPtr& sourceSchema,
        const NTableClient::TTableSchemaPtr& targetSchema) = 0;

    //! Does the same as Get(sourceSchema, targetSchema)->Convert(row) but with less atomic operations.
    virtual TCompactUnversionedOwningRow Convert(
        const TCompactUnversionedOwningRow& row,
        const NTableClient::TTableSchemaPtr& sourceSchema,
        const NTableClient::TTableSchemaPtr& targetSchema) = 0;

    //! Convenience overloads — same conversion, with strong typedefs preserved.
    TPayload Convert(
        const TPayload& payload,
        const NTableClient::TTableSchemaPtr& sourceSchema,
        const NTableClient::TTableSchemaPtr& targetSchema)
    {
        return TPayload(Convert(payload.Underlying(), sourceSchema, targetSchema));
    }

    TKey Convert(
        const TKey& key,
        const NTableClient::TTableSchemaPtr& sourceSchema,
        const NTableClient::TTableSchemaPtr& targetSchema)
    {
        return TKey(Convert(key.Underlying(), sourceSchema, targetSchema));
    }
};

DEFINE_REFCOUNTED_TYPE(IPayloadConverterCache)

////////////////////////////////////////////////////////////////////////////////

IPayloadConverterCachePtr CreatePayloadConverterCache(
    NQueryClient::IColumnEvaluatorCachePtr evaluatorCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
