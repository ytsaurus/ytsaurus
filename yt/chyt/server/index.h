#pragma once

#include "private.h"

#include <Storages/MergeTree/MergeTreeIndices.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! A convenient wrapper for CH index.
//! It combines all parts of the index together (description, condition, etc).
//! This wrapper guarantees lifetime of description.
class TClickHouseIndex
    : public TRefCounted
{
public:
    TClickHouseIndex(
        DB::IndexDescription description,
        const DB::SelectQueryInfo& selectQuery,
        DB::ContextPtr context);

    DEFINE_BYREF_RO_PROPERTY(DB::IndexDescription, Description);
    DEFINE_BYREF_RO_PROPERTY(DB::MergeTreeIndexPtr, Index);
    DEFINE_BYREF_RO_PROPERTY(DB::MergeTreeIndexConditionPtr, Condition);

    DB::MergeTreeIndexAggregatorPtr CreateAggregator() const;
};

DEFINE_REFCOUNTED_TYPE(TClickHouseIndex)

////////////////////////////////////////////////////////////////////////////////

//! A helper for creating indexes.
//! It stores all nessesary information about the query.
class TClickHouseIndexBuilder : DB::WithContext
{
public:
    TClickHouseIndexBuilder(
        const DB::SelectQueryInfo* query,
        DB::ContextPtr context_);

    TClickHouseIndexPtr CreateIndex(
        DB::NamesAndTypesList namesAndTypes,
        TString indexType) const;

private:
    const DB::SelectQueryInfo* Query_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
