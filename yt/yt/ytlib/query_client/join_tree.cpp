#include "join_tree.h"

#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/library/query/base/query_helpers.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NQueryClient {

using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTableClient;

using TColumnSet = THashSet<TString>;
using TTypeLookup = THashMap<TString, TLogicalTypePtr>;

////////////////////////////////////////////////////////////////////////////////

class TReferenceHarvester
    : public TVisitor<TReferenceHarvester>
{
public:
    explicit TReferenceHarvester(TColumnSet* storage)
        : Storage_(storage)
    { }

    void OnReference(const TReferenceExpression* referenceExpr)
    {
        Storage_->insert(referenceExpr->ColumnName);
    }

private:
    TColumnSet* Storage_;
};

////////////////////////////////////////////////////////////////////////////////

TConstGroupClausePtr ResetCommonPrefixInGroupClause(const TConstGroupClausePtr& group)
{
    if (!group || group->CommonPrefixWithPrimaryKey == 0) {
        return group;
    }

    auto modifiedGroup = New<TGroupClause>();
    modifiedGroup->GroupItems = group->GroupItems;
    modifiedGroup->AggregateItems = group->AggregateItems;
    modifiedGroup->TotalsMode = group->TotalsMode;
    modifiedGroup->CommonPrefixWithPrimaryKey = 0;

    return modifiedGroup;
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJoinTreeNode)

class TJoinTreeNode
    : public IJoinTreeNode
{
public:
    struct TLeaf
    {
        const TTableId TableId;
        const TMappedSchema Schema;
    };

    struct TInner
    {
        const TJoinTreeNodePtr Left;
        const TJoinTreeNodePtr Right;
        const TJoinClausePtr MutableJoin;

        TColumnSet NeededColumns;
    };

    std::variant<TInner, TLeaf> Body;
    TQueryPtr MutableQuery;
    TProjectClausePtr MutableProject;
    TOrderClausePtr MutableOrder;
    TColumnSet AvailableColumns;

    TJoinTreeNode(TJoinTreeNodePtr left, TJoinTreeNodePtr right)
        : Body(TInner{std::move(left), std::move(right), New<TJoinClause>(), {}})
        , MutableQuery(New<TQuery>())
    { }

    TJoinTreeNode(
        TTableId tableId,
        const TMappedSchema& schema,
        bool shuffled)
        : Body(TLeaf{tableId, schema})
        , MutableQuery(shuffled ? New<TQuery>() : nullptr)
    {
        if (shuffled) {
            MutableQuery->Schema = schema;
        }
    }

    virtual TConstQueryPtr GetQuery() const override
    {
        return MutableQuery;
    }

    virtual std::optional<TTableId> GetTableId() const override
    {
        if (auto* leafNode = std::get_if<TLeaf>(&Body)) {
            return leafNode->TableId;
        }

        return std::nullopt;
    }

    virtual std::pair<IJoinTreeNodePtr, IJoinTreeNodePtr> GetChildren() const override
    {
        if (auto* innerNode = std::get_if<TInner>(&Body)) {
            return {innerNode->Left, innerNode->Right};
        }
        return {};
    }

    const TColumnSet& PullUpAvailableColumns()
    {
        return *Visit(
            Body,
            [&] (const TLeaf& leafNode) {
                for (const auto& desc : leafNode.Schema.Mapping) {
                    AvailableColumns.insert(desc.Name);
                }
                return &AvailableColumns;
            },
            [&] (const TInner& innerNode) {
                const auto& leftAvailable = innerNode.Left->PullUpAvailableColumns();
                const auto& rightAvailable = innerNode.Right->PullUpAvailableColumns();

                AvailableColumns.insert(leftAvailable.begin(), leftAvailable.end());
                AvailableColumns.insert(rightAvailable.begin(), rightAvailable.end());
                return &AvailableColumns;
            });
    }

    void AddJoinEquationsToChildrenProject()
    {
        auto* innerNode = std::get_if<TInner>(&Body);
        if (!innerNode) {
            return;
        }

        auto& join = innerNode->MutableJoin;
        innerNode->Left->MutableProject = New<TProjectClause>();
        innerNode->Left->MutableOrder = New<TOrderClause>();

        for (int index = 0; index < static_cast<int>(join->CommonKeyPrefix); ++index) {
            const auto& expr = join->SelfEquations[index].Expression;

            auto name = InferName(expr);
            innerNode->Left->MutableProject->AddProjection(expr, name);
            innerNode->Left->MutableOrder->OrderItems.push_back({.Expression = expr, .Descending = false});

            join->SelfEquations[index] = {
                .Expression = New<TReferenceExpression>(expr->LogicalType, std::move(name)),
                .Evaluated = false,
            };
        }

        innerNode->Left->AddJoinEquationsToChildrenProject();
        innerNode->Right->AddJoinEquationsToChildrenProject();
    }

    void CollectAndPushDownNeededColumns(const TColumnSet& neededColumns)
    {
        auto* inner = std::get_if<TInner>(&Body);
        if (!inner) {
            return;
        }

        for (const auto& column : neededColumns) {
            if (AvailableColumns.contains(column)) {
                inner->NeededColumns.insert(column);
            }
        }

        TReferenceHarvester harvester(&inner->NeededColumns);

        if (MutableProject) {
            for (const auto& projection : MutableProject->Projections) {
                harvester.Visit(projection.Expression);
            }
        }

        if (auto where = MutableQuery->WhereClause) {
            harvester.Visit(where);
        }

        if (auto group = MutableQuery->GroupClause) {
            for (const auto& groupItem : group->GroupItems) {
                harvester.Visit(groupItem.Expression);
            }
            for (const auto& aggregateItem : group->AggregateItems) {
                for (const auto& argument : aggregateItem.Arguments) {
                    harvester.Visit(argument);
                }
            }
        }

        auto& join = inner->MutableJoin;
        for (int index = join->CommonKeyPrefix; index < std::ssize(join->SelfEquations); ++index)
        {
            harvester.Visit(join->SelfEquations[index].Expression);
            harvester.Visit(join->ForeignEquations[index]);
        }
        MutableQuery->JoinClauses = {std::move(join)};

        inner->Left->CollectAndPushDownNeededColumns(inner->NeededColumns);
        inner->Right->CollectAndPushDownNeededColumns(inner->NeededColumns);
    }

    void AddParentNeededColumnsToProject(const TColumnSet& neededColumns, const TTypeLookup& typeLookup)
    {
        if (!MutableQuery) {
            return;
        }

        if (!MutableQuery->GroupClause) {
            if (!MutableProject) {
                MutableProject = New<TProjectClause>();
            }

            for (const auto& column : neededColumns) {
                if (!AvailableColumns.contains(column)) {
                    continue;
                }

                auto it = std::find_if(
                    MutableProject->Projections.begin(),
                    MutableProject->Projections.end(),
                    [&] (const TNamedItem& item) {
                        return item.Name == column;
                    });
                if (it == MutableProject->Projections.end()) {
                    MutableProject->AddProjection(New<TReferenceExpression>(typeLookup.at(column), column), column);
                }
            }
        }

        MutableQuery->ProjectClause = std::move(MutableProject);
        MutableQuery->OrderClause = std::move(MutableOrder);

        if (auto* innerNode = std::get_if<TInner>(&Body)) {
            innerNode->Left->AddParentNeededColumnsToProject(innerNode->NeededColumns, typeLookup);
            innerNode->Right->AddParentNeededColumnsToProject(innerNode->NeededColumns, typeLookup);
        }
    }

    void SetReadSchemas()
    {
        if (auto* innerNode = std::get_if<TInner>(&Body)) {
            innerNode->Left->SetReadSchemas();
            innerNode->Right->SetReadSchemas();

            MutableQuery->Schema = innerNode->Left->GetOutSchema();
            MutableQuery->Schema = innerNode->Right->GetOutSchema();
        }
    }

    TMappedSchema GetOutSchema() const
    {
        if (MutableQuery) {
            auto columns = MutableQuery->GetTableSchema()->Columns();
            int orderedPrefix = MutableQuery->OrderClause
                ? MutableQuery->OrderClause->OrderItems.size()
                : 0;

            TMappedSchema mappedSchema;

            for (int index = 0; index < orderedPrefix; ++index) {
                columns[index].SetSortOrder(ESortOrder::Ascending);
            }

            mappedSchema.Mapping.reserve(columns.size());

            for (int index = 0; index < std::ssize(columns); ++index) {
                mappedSchema.Mapping.push_back({.Name = columns[index].Name(), .Index = index});
            }

            mappedSchema.Original = New<TTableSchema>(std::move(columns));

            return mappedSchema;
        } else if (auto* leaf = std::get_if<TLeaf>(&Body)) {
            return leaf->Schema;
        } else {
            YT_ABORT();
        }
    }

    void Cleanup()
    {
        AvailableColumns.clear();

        if (auto* innerNode = std::get_if<TInner>(&Body)) {
            innerNode->NeededColumns.clear();
            innerNode->Left->Cleanup();
            innerNode->Right->Cleanup();
        }
    }

    static TColumnSet GetColumnsNeededByTopQuery(const TConstQueryPtr& original)
    {
        TColumnSet columnsForTopQuery;
        if (!original->GroupClause) {
            if (auto order = original->OrderClause) {
                for (const auto& item : order->OrderItems) {
                    TReferenceHarvester(&columnsForTopQuery).Visit(item.Expression);
                }
            }

            if (auto project = original->ProjectClause) {
                for (const auto& item : project->Projections) {
                    TReferenceHarvester(&columnsForTopQuery).Visit(item.Expression);
                }
            } else {
                auto tableSchema = original->GetTableSchema();
                for (const auto& column : tableSchema->Columns()) {
                    columnsForTopQuery.insert(column.Name());
                }
            }
        }

        return columnsForTopQuery;
    }
};

DEFINE_REFCOUNTED_TYPE(TJoinTreeNode)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJoinTree)

class TJoinTree
    : public IJoinTree
{
public:
    TJoinTree(TConstQueryPtr query, TJoinTreeNodePtr root)
        : OriginalQuery_(std::move(query))
        , Root_(std::move(root))
    { }

    void Prepare() const
    {
        TTypeLookup typeLookup;
        PopulateTypeLookupFromSchema(OriginalQuery_->Schema, &typeLookup);
        for (const auto& joinClause : OriginalQuery_->JoinClauses) {
            PopulateTypeLookupFromSchema(joinClause->Schema, &typeLookup);
        }

        Root_->PullUpAvailableColumns();

        Root_->AddJoinEquationsToChildrenProject();

        auto neededColumns = Root_->GetColumnsNeededByTopQuery(OriginalQuery_);

        Root_->CollectAndPushDownNeededColumns(neededColumns);

        Root_->AddParentNeededColumnsToProject(neededColumns, typeLookup);

        Root_->SetReadSchemas();

        Root_->Cleanup();
    }

    virtual IJoinTreeNodePtr GetRoot() const
    {
        return Root_;
    }

private:
    static void PopulateTypeLookupFromSchema(const TMappedSchema& schema, TTypeLookup* typeLookup)
    {
        for (const auto& descriptor : schema.Mapping) {
            const auto& column = schema.Original->Columns()[descriptor.Index];
            (*typeLookup)[descriptor.Name] = column.LogicalType();
        }
    }

    TConstQueryPtr OriginalQuery_;
    TJoinTreeNodePtr Root_;
};

DEFINE_REFCOUNTED_TYPE(TJoinTree)

////////////////////////////////////////////////////////////////////////////////

bool IsIndexJoinQuery(const TConstQueryPtr& query)
{
    if (query->JoinClauses.empty()) {
        return false;
    }

    for (const auto& joinClause : query->JoinClauses) {
        if (joinClause->ForeignKeyPrefix == 0 || joinClause->IsLeft) {
            return false;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

IJoinTreePtr MakeIndexJoinTree(const TConstQueryPtr& query, const TDataSource& primaryTable)
{
    THROW_ERROR_EXCEPTION_IF(!IsIndexJoinQuery(query),
        "Query is not a valid index join");

    auto root = New<TJoinTreeNode>(
        primaryTable.ObjectId,
        query->Schema,
        /*shuffled*/ true);
    auto totalPredicate = query->WhereClause;

    for (const auto& joinClause : query->JoinClauses) {
        root = New<TJoinTreeNode>(std::move(root), New<TJoinTreeNode>(
            joinClause->ForeignObjectId,
            joinClause->Schema,
            /*shuffled*/ false));

        const auto& newJoin = std::get<TJoinTreeNode::TInner>(root->Body).MutableJoin;
        newJoin->CommonKeyPrefix = joinClause->ForeignKeyPrefix;
        newJoin->ForeignKeyPrefix = joinClause->ForeignKeyPrefix;

        if (joinClause->Predicate) {
            totalPredicate  = totalPredicate
                ? MakeAndExpression(totalPredicate, joinClause->Predicate)
                : joinClause->Predicate;
        }

        newJoin->ForeignEquations = joinClause->ForeignEquations;
        newJoin->SelfEquations = joinClause->SelfEquations;
        for (auto& [expr, evaluated] : newJoin->SelfEquations) {
            if (evaluated) {
                evaluated = false;
                expr = TSelfifyRewriter{.JoinClause = joinClause}.Visit(expr);
            }
        }
    }

    root->MutableQuery->WhereClause = totalPredicate;
    root->MutableQuery->GroupClause = ResetCommonPrefixInGroupClause(query->GroupClause);

    auto joinTree = New<TJoinTree>(query, std::move(root));
    joinTree->Prepare();

    return joinTree;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
