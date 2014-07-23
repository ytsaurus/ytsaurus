#include "stdafx.h"
#include "plan_node.h"
#include "plan_visitor.h"
#include "plan_helpers.h"
#include "plan_context.h"
#include "helpers.h"

#include <core/misc/protobuf_helpers.h>

#include <ytlib/query_client/operator.pb.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/unversioned_row.h>

namespace NYT {
namespace NQueryClient {

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TOperator* TOperator::CloneImpl(TPlanContext* context) const
{
    TOperator* result = nullptr;

    switch (GetKind()) {
        case EOperatorKind::Scan:
            result = new (context) TScanOperator(context, *this->As<TScanOperator>());
            break;
        case EOperatorKind::Filter:
            result = new (context) TFilterOperator(context, *this->As<TFilterOperator>());
            break;
        case EOperatorKind::Group:
            result = new (context) TGroupOperator(context, *this->As<TGroupOperator>());
            break;
        case EOperatorKind::Project:
            result = new (context) TProjectOperator(context, *this->As<TProjectOperator>());
            break;
    }

    YCHECK(result);
    return result;
}

const TTableSchema& TScanOperator::GetTableSchema(bool ignoreCache) const
{
    if (!TableSchema_ || ignoreCache) {
        TableSchema_ = std::make_unique<TTableSchema>(GetTableSchemaFromDataSplit(DataSplits()[0]));
        // TODO(lukyan): check that other splits have the same table scheme
    }
    return *TableSchema_;
}

const TTableSchema& TFilterOperator::GetTableSchema(bool ignoreCache) const
{
    return GetSource()->GetTableSchema(ignoreCache);
}

void CheckTableScheme(const TTableSchema& tableScheme)
{
    yhash_set<Stroka> columnNames;
    for (const auto& column : tableScheme.Columns()) {
        if (!columnNames.insert(column.Name).second) {
            THROW_ERROR_EXCEPTION("Redefinition of column %s", column.Name.Quote());
        }
    }
}

const TTableSchema& TGroupOperator::GetTableSchema(bool ignoreCache) const
{
    if (!TableSchema_ || ignoreCache) {
        TableSchema_ = std::make_unique<TTableSchema>();
        TTableSchema& result = *TableSchema_;

        auto sourceSchema = GetSource()->GetTableSchema();
        for (const auto& groupItem : GroupItems()) {
            result.Columns().emplace_back(
                groupItem.Name,
                InferType(groupItem.Expression, sourceSchema));
        }

        for (const auto& aggregateItem : AggregateItems()) {
            result.Columns().emplace_back(
                aggregateItem.Name,
                InferType(aggregateItem.Expression, sourceSchema));
        }

        CheckTableScheme(result);
    }
    return *TableSchema_;
}

const TTableSchema& TProjectOperator::GetTableSchema(bool ignoreCache) const
{
    if (!TableSchema_ || ignoreCache) {
        TableSchema_ = std::make_unique<TTableSchema>();
        TTableSchema& result = *TableSchema_;

        auto sourceSchema = GetSource()->GetTableSchema();
        for (const auto& projection : Projections()) {
            result.Columns().emplace_back(
                projection.Name,
                InferType(projection.Expression, sourceSchema));
        }

        CheckTableScheme(result);
    }
    return *TableSchema_;
}

TKeyColumns TOperator::GetKeyColumns() const
{
    return InferKeyColumns(this);
}

TKeyRange TOperator::GetKeyRange() const
{
    return InferKeyRange(this);
}

NVersionedTableClient::TNameTablePtr TOperator::GetNameTable() const
{
    return NVersionedTableClient::TNameTable::FromSchema(GetTableSchema());
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TNamedExpression* serialized, const TNamedExpression& original)
{
    ToProto(serialized->mutable_expression(), original.Expression);
    ToProto(serialized->mutable_name(), original.Name);
}

void ToProto(NProto::TAggregateItem* serialized, const TAggregateItem& original)
{
    ToProto(serialized->mutable_expression(), original.Expression);
    serialized->set_aggregate_function(original.AggregateFunction);
    ToProto(serialized->mutable_name(), original.Name);
}

void ToProto(NProto::TOperator* serialized, const TOperator* original)
{
    serialized->set_kind(original->GetKind());

    switch (original->GetKind()) {

        case EOperatorKind::Scan: {
            auto* op = original->As<TScanOperator>();
            auto* proto = serialized->MutableExtension(NProto::TScanOperator::scan_operator);
            ToProto(proto->mutable_data_split(), op->DataSplits());
            break;
        }

        case EOperatorKind::Filter: {
            auto* op = original->As<TFilterOperator>();
            auto* proto = serialized->MutableExtension(NProto::TFilterOperator::filter_operator);
            ToProto(proto->mutable_source(), op->GetSource());
            ToProto(proto->mutable_predicate(), op->GetPredicate());
            break;
        }

        case EOperatorKind::Group: {
            auto* op = original->As<TGroupOperator>();
            auto* proto = serialized->MutableExtension(NProto::TGroupOperator::group_operator);
            ToProto(proto->mutable_source(), op->GetSource());
            ToProto(proto->mutable_group_items(), op->GroupItems());
            ToProto(proto->mutable_aggregate_items(), op->AggregateItems());
            break;
        }

        case EOperatorKind::Project: {
            auto* op = original->As<TProjectOperator>();
            auto* proto = serialized->MutableExtension(NProto::TProjectOperator::project_operator);
            ToProto(proto->mutable_source(), op->GetSource());
            ToProto(proto->mutable_projections(), op->Projections());
            break;
        }

    }
}

TNamedExpression FromProto(const NProto::TNamedExpression& serialized, TPlanContext* context)
{
    return TNamedExpression(
        FromProto(serialized.expression(), context),
        serialized.name());
}

TAggregateItem FromProto(const NProto::TAggregateItem& serialized, TPlanContext* context)
{
    return TAggregateItem(
        FromProto(serialized.expression(), context),
        EAggregateFunctions(serialized.aggregate_function()),
        serialized.name());
}

const TOperator* FromProto(const NProto::TOperator& serialized, TPlanContext* context)
{
    const TOperator* result = nullptr;

    switch (EOperatorKind(serialized.kind())) {

        case EOperatorKind::Scan: {
            auto data = serialized.GetExtension(NProto::TScanOperator::scan_operator);
            auto typedResult = new (context) TScanOperator(context);
            typedResult->DataSplits().reserve(data.data_split_size());

            for (int i = 0; i < data.data_split_size(); ++i) {
                TDataSplit dataSplit;
                FromProto(&dataSplit, data.data_split(i));
                typedResult->DataSplits().push_back(dataSplit);
            }
            YASSERT(!result);
            result = typedResult;
            break;
        }

        case EOperatorKind::Filter: {
            auto data = serialized.GetExtension(NProto::TFilterOperator::filter_operator);
            auto typedResult = new (context) TFilterOperator(
                context,
                FromProto(data.source(), context));
            typedResult->SetPredicate(FromProto(data.predicate(), context));
            YASSERT(!result);
            result = typedResult;
            break;
        }

        case EOperatorKind::Group: {
            auto data = serialized.GetExtension(NProto::TGroupOperator::group_operator);
            auto typedResult = new (context) TGroupOperator(
                context,
                FromProto(data.source(), context));
            typedResult->GroupItems().reserve(data.group_items_size());
            for (int i = 0; i < data.group_items_size(); ++i) {
                typedResult->GroupItems().push_back(
                    FromProto(data.group_items(i), context));
            }
            typedResult->AggregateItems().reserve(data.aggregate_items_size());
            for (int i = 0; i < data.aggregate_items_size(); ++i) {
                typedResult->AggregateItems().push_back(
                    FromProto(data.aggregate_items(i), context));
            }
            YASSERT(!result);
            result = typedResult;
            break;
        }

        case EOperatorKind::Project: {
            auto data = serialized.GetExtension(NProto::TProjectOperator::project_operator);
            auto typedResult = new (context) TProjectOperator(
                context,
                FromProto(data.source(), context));
            typedResult->Projections().reserve(data.projections_size());
            for (int i = 0; i < data.projections_size(); ++i) {
                typedResult->Projections().push_back(
                    FromProto(data.projections(i), context));
            }
            YASSERT(!result);
            result = typedResult;
            break;
        }

    }

    YCHECK(result);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

