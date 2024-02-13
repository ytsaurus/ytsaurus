#include "index.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

DB::IndexDescription CreateClickHouseIndexDescription(
    DB::NamesAndTypesList namesAndTypes,
    TString indexType)
{
    if (indexType != "minmax" && indexType != "set") {
        THROW_ERROR_EXCEPTION("Unexpected index type %Qv", indexType);
    }

    DB::IndexDescription description;

    // Name is not used, but it should be non-empty.
    description.name = "YT";

    description.type = indexType;

    for (const auto& [name, type] : namesAndTypes) {
        description.column_names.emplace_back(name);
        description.data_types.emplace_back(type);
        description.sample_block.insert({type->createColumn(), type, name});
    }

    // We support only indexes on columns, not on complex expressions (e.g. 'a * b').
    // So create an identity transformation for columns (x -> x).
    description.expression = std::make_shared<DB::ExpressionActions>(
        std::make_shared<DB::ActionsDAG>(namesAndTypes));

    if (indexType == "set") {
        // 'max_rows' - how many different values one granule can contain. 0 stands for unlimited.
        description.arguments.emplace_back(/*max_rows*/ 0u);
    }

    // This call is unnecessary. It only validates that we filled arguments in the description correctly.
    // 'attach' is used only for bloomFilterNew, it's meaningless here.
    DB::MergeTreeIndexFactory::instance().validate(description, /*attach*/ false);

    return description;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TClickHouseIndex::TClickHouseIndex(
    DB::IndexDescription description,
    const DB::SelectQueryInfo& query,
    DB::ContextPtr context)
    : Description_(std::move(description))
    , Index_(DB::MergeTreeIndexFactory::instance().get(Description_))
    , Condition_(Index_->createIndexCondition(query, context))
{ }

DB::MergeTreeIndexAggregatorPtr TClickHouseIndex::CreateAggregator() const
{
    return Index_->createIndexAggregator();
}

////////////////////////////////////////////////////////////////////////////////

TClickHouseIndexBuilder::TClickHouseIndexBuilder(
    const DB::SelectQueryInfo* query,
    DB::ContextPtr context_)
    : DB::WithContext(context_)
    , Query_(query)
{ }

TClickHouseIndexPtr TClickHouseIndexBuilder::CreateIndex(
    DB::NamesAndTypesList namesAndTypes,
    TString indexType) const
{
    return New<TClickHouseIndex>(
        NDetail::CreateClickHouseIndexDescription(namesAndTypes, indexType),
        *Query_,
        getContext());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
