#include "yql_generic_provider_impl.h"

#include <yql/essentials/ast/yql_type_string.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <contrib/ydb/library/yql/providers/dq/mkql/parser.h>
#include <yql/essentials/providers/common/provider/yql_data_provider_impl.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <contrib/ydb/library/yql/providers/common/pushdown/type_ann.h>
#include <contrib/ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <yql/essentials/utils/log/log.h>

// You may want to change AST, graph nodes, types, but finally you'll
// return to the existing structure, inherited from ClickHouse and S3 providers.
// In this case please increment this counter:
// Hours wasted: 5

namespace NYql {

    using namespace NNodes;

    class TGenericDataSourceTypeAnnotationTransformer: public TVisitorTransformerBase {
    public:
        TGenericDataSourceTypeAnnotationTransformer(TGenericState::TPtr state)
            : TVisitorTransformerBase(true)
            , State_(state)
        {
            using TSelf = TGenericDataSourceTypeAnnotationTransformer;
            AddHandler({TCoConfigure::CallableName()}, Hndl(&TSelf::HandleConfig));
            AddHandler({TGenTable::CallableName()}, Hndl(&TSelf::HandleTable));
            AddHandler({TGenReadTable::CallableName()}, Hndl(&TSelf::HandleReadTable));
            AddHandler({TGenSourceSettings::CallableName()}, Hndl(&TSelf::HandleSourceSettings));
        }

        TStatus HandleConfig(const TExprNode::TPtr& input, TExprContext& ctx) {
            if (!EnsureMinArgsCount(*input, 2, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureWorldType(*input->Child(TCoConfigure::idx_World), ctx)) {
                return TStatus::Error;
            }

            if (!EnsureSpecificDataSource(*input->Child(TCoConfigure::idx_DataSource), GenericProviderName, ctx)) {
                return TStatus::Error;
            }

            input->SetTypeAnn(input->Child(TCoConfigure::idx_World)->GetTypeAnn());
            return TStatus::Ok;
        }

        TStatus HandleTable(const TExprNode::TPtr& input, TExprContext& ctx) {
            if (!EnsureArgsCount(*input, 1, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureAtom(*input->Child(TGenTable::idx_Name), ctx)) {
                return TStatus::Error;
            }

            input->SetTypeAnn(ctx.MakeType<TUnitExprType>());

            return TStatus::Ok;
        }

        TStatus HandleSourceSettings(const TExprNode::TPtr& input, TExprContext& ctx) {
            if (!EnsureArgsCount(*input, 6, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureWorldType(*input->Child(TGenSourceSettings::idx_World), ctx)) {
                return TStatus::Error;
            }

            if (!EnsureAtom(*input->Child(TGenSourceSettings::idx_Cluster), ctx)) {
                return TStatus::Error;
            }

            if (!EnsureAtom(*input->Child(TGenSourceSettings::idx_Table), ctx)) {
                return TStatus::Error;
            }

            if (input->ChildrenSize() > TGenSourceSettings::idx_Token &&
                !TCoSecureParam::Match(input->Child(TGenSourceSettings::idx_Token))) {
                ctx.AddError(TIssue(ctx.GetPosition(input->Child(TGenSourceSettings::idx_Token)->Pos()),
                                    TStringBuilder() << "Expected " << TCoSecureParam::CallableName()));
                return TStatus::Error;
            }

            // Find requested table metadata
            TString clusterName{input->Child(TGenSourceSettings::idx_Cluster)->Content()};
            TString tableName{input->Child(TGenSourceSettings::idx_Table)->Content()};

            THashSet<TStringBuf> columnSet;
            for (const auto& child : input->Child(TGenSourceSettings::idx_Columns)->Children()) {
                columnSet.insert(child->Content());
            }

            auto [tableMeta, issues] = State_->GetTable({clusterName, tableName});
            if (issues) {
                for (const auto& issue : issues) {
                    ctx.AddError(issue);
                }
                return TStatus::Error;
            }

            // Create type annotation
            TVector<const TItemExprType*> blockRowTypeItems;

            const auto structExprType = tableMeta->ItemType;
            for (const auto& item : structExprType->GetItems()) {
                // Filter out columns that are not required in this query
                if (columnSet.contains(item->GetName())) {
                    blockRowTypeItems.push_back(
                        ctx.MakeType<TItemExprType>(item->GetName(), ctx.MakeType<TBlockExprType>(item->GetItemType())));
                }
            }

            // Filter
            const TStatus filterAnnotationStatus = NYql::NPushdown::AnnotateFilterPredicate(input, TGenSourceSettings::idx_FilterPredicate, structExprType, ctx);
            if (filterAnnotationStatus != TStatus::Ok) {
                return filterAnnotationStatus;
            }

            blockRowTypeItems.push_back(ctx.MakeType<TItemExprType>(
                BlockLengthColumnName, ctx.MakeType<TScalarExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint64))));
            const TTypeAnnotationNode* typeAnnotationNode = ctx.MakeType<TStructExprType>(blockRowTypeItems);

            YQL_CLOG(DEBUG, ProviderGeneric) << "struct column order" << (static_cast<const TStructExprType*>(typeAnnotationNode))->ToString();

            auto streamExprType = ctx.MakeType<TStreamExprType>(typeAnnotationNode);
            input->SetTypeAnn(streamExprType);

            return TStatus::Ok;
        }

        TStatus HandleReadTable(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
            Y_UNUSED(output);
            if (!EnsureArgsCount(*input, 5, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureWorldType(*input->Child(TGenReadTable::idx_World), ctx)) {
                return TStatus::Error;
            }

            if (!EnsureSpecificDataSource(*input->Child(TGenReadTable::idx_DataSource), GenericProviderName, ctx)) {
                return TStatus::Error;
            }

            if (!EnsureCallable(*input->Child(TGenReadTable::idx_Table), ctx)) {
                return TStatus::Error;
            }

            TMaybe<THashSet<TStringBuf>> columnSet;
            auto columns = input->Child(TGenReadTable::idx_Columns);
            if (!columns->IsCallable(TCoVoid::CallableName())) {
                if (!EnsureTuple(*columns, ctx)) {
                    return TStatus::Error;
                }

                columnSet.ConstructInPlace();
                for (auto& child : columns->Children()) {
                    if (!EnsureAtom(*child, ctx)) {
                        return TStatus::Error;
                    }

                    auto name = child->Content();
                    if (!columnSet->insert(name).second) {
                        ctx.AddError(
                            TIssue(ctx.GetPosition(child->Pos()), TStringBuilder() << "Duplicated column name: " << name));
                        return TStatus::Error;
                    }
                }
            }

            // Determine cluster name
            TString clusterName{input->Child(TGenReadTable::idx_DataSource)->Child(1)->Content()};

            // Determine table name
            const auto tableNode = input->Child(TGenReadTable::idx_Table);
            if (!TGenTable::Match(tableNode)) {
                ctx.AddError(TIssue(ctx.GetPosition(tableNode->Pos()),
                                    TStringBuilder() << "Expected " << TGenTable::CallableName()));
                return TStatus::Error;
            }

            TGenTable table(tableNode);
            const auto tableName = table.Name().StringValue();

            // Extract table metadata
            auto [tableMeta, issues] = State_->GetTable({clusterName, tableName});
            if (issues) {
                for (const auto& issue : issues) {
                    ctx.AddError(issue);
                }
                return TStatus::Error;
            }

            auto itemType = tableMeta->ItemType;
            auto columnOrder = tableMeta->ColumnOrder;

            if (columnSet) {
                YQL_CLOG(INFO, ProviderGeneric) << "custom column set" << ColumnSetToString(*columnSet.Get());

                TVector<const TItemExprType*> items = itemType->GetItems();
                EraseIf(items, [&](const TItemExprType* item) { return !columnSet->contains(item->GetName()); });
                EraseIf(columnOrder, [&](const TString& col) { return !columnSet->contains(col); });
                itemType = ctx.MakeType<TStructExprType>(items);

                YQL_CLOG(DEBUG, ProviderGeneric) << "struct column order" << (static_cast<const TStructExprType*>(itemType))->ToString();
            }

            // Filter
            const TStatus filterAnnotationStatus = NYql::NPushdown::AnnotateFilterPredicate(input, TGenReadTable::idx_FilterPredicate, itemType, ctx);
            if (filterAnnotationStatus != TStatus::Ok) {
                return filterAnnotationStatus;
            }

            input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
                input->Child(TGenReadTable::idx_World)->GetTypeAnn(), ctx.MakeType<TListExprType>(itemType)}));

            return State_->Types->SetColumnOrder(*input, TColumnOrder(columnOrder), ctx);
        }

        TString ColumnOrderToString(const TVector<TString>& columns) {
            TStringBuilder sb;

            for (std::size_t i = 0; i < columns.size(); i++) {
                sb << i << "=" << columns[i];
                if (i != columns.size() - 1) {
                    sb << ", ";
                }
            }

            return sb;
        }

        TString ColumnSetToString(const THashSet<TStringBuf>& columnSet) {
            TStringBuilder sb;

            std::size_t i = 0;
            for (const auto key : columnSet) {
                sb << i << "=" << key;
                if (i != columnSet.size() - 1) {
                    sb << ", ";
                }
                i++;
            }

            return sb;
        }

    private:
        TGenericState::TPtr State_;
    };

    THolder<TVisitorTransformerBase> CreateGenericDataSourceTypeAnnotationTransformer(TGenericState::TPtr state) {
        return MakeHolder<TGenericDataSourceTypeAnnotationTransformer>(state);
    }

} // namespace NYql
