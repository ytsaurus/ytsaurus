#include "yql_ytflow_provider_impl.h"
#include "yql_ytflow_utils.h"

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

#include <yt/yql/providers/ytflow/expr_nodes/yql_ytflow_expr_nodes.h>


namespace NYql {

using namespace NNodes;


class TYtflowDataSourceTypeAnnotationTransformer: public TVisitorTransformerBase {
public:
    TYtflowDataSourceTypeAnnotationTransformer(TYtflowState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(std::move(state))
    {
#define ADD_HANDLER(nodeType, method) \
    AddHandler({nodeType::CallableName()}, Hndl(&TYtflowDataSourceTypeAnnotationTransformer::method))

        ADD_HANDLER(TCoConfigure, HandleConfigure);
        ADD_HANDLER(TYtflowReadStub, HandleReadStub);
        ADD_HANDLER(TYtflowReadWrap, HandleReadWrap);
        ADD_HANDLER(TYtflowPersistentSource, HandlePersistentSource);

#undef ADD_HANDLER
    }

private:
    TStatus HandleConfigure(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinArgsCount(*input, 2, ctx)) {
            return TStatus::Error;
        }

        auto* world = input->Child(TCoConfigure::idx_World);
        if (!EnsureWorldType(*world, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureSpecificDataSource(*input->Child(TCoConfigure::idx_DataSource), YtflowProviderName, ctx)) {
            return TStatus::Error;
        }

        input->SetTypeAnn(world->GetTypeAnn());

        return TStatus::Ok;
    }

    TStatus HandleReadWrap(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureMinMaxArgsCount(*input, 1, 2, ctx)) {
            return TStatus::Error;
        }

        auto* inputChild = input->Child(TYtflowReadWrap::idx_Input);
        if (!EnsureTupleTypeSize(*inputChild, 2, ctx)) {
            return TStatus::Error;
        }

        auto readType = inputChild->GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back();
        if (!EnsureListType(inputChild->Pos(), *readType, ctx)) {
            return TStatus::Error;
        }

        auto* itemType = readType->Cast<TListExprType>()->GetItemType();
        if (!EnsureStructType(inputChild->Pos(), *itemType, ctx)) {
            return TStatus::Error;
        }

        if (input->ChildrenSize() > TYtflowReadWrap::idx_Token && !TCoSecureParam::Match(input->Child(TYtflowReadWrap::idx_Token))) {
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Expect SecureParam but got: "
                << input->Child(TYtflowReadWrap::idx_Token)->Content()));
            return TStatus::Error;
        }

        input->SetTypeAnn(readType);

        return TStatus::Ok;
    }

    TStatus HandlePersistentSource(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureAtom(*input->Child(TYtflowPersistentSource::idx_Name), ctx)) {
            return TStatus::Error;
        }

        auto* readWrapInputChild = input->Child(TYtflowPersistentSource::idx_Input);
        input->SetTypeAnn(readWrapInputChild->GetTypeAnn());

        return TStatus::Ok;
    }

    TStatus HandleReadStub(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 2, ctx)) {
            return TStatus::Error;
        }

        if (!EnsureWorldType(*input->Child(TYtflowReadStub::idx_World), ctx)) {
            return TStatus::Error;
        }

        auto* itemTypeChild = input->Child(TYtflowReadStub::idx_ItemType);
        if (!EnsureTypeWithStructType(*itemTypeChild, ctx)) {
            return TStatus::Error;
        }

        auto* itemType = itemTypeChild->GetTypeAnn()->Cast<TTypeExprType>()->GetType();
        input->SetTypeAnn(ctx.MakeType<TListExprType>(itemType));

        return TStatus::Ok;
    }

private:
    TYtflowState::TPtr State_;
};

THolder<TVisitorTransformerBase> CreateYtflowDataSourceTypeAnnotationTransformer(TYtflowState::TPtr state) {
    return MakeHolder<TYtflowDataSourceTypeAnnotationTransformer>(std::move(state));
}

} // namespace NYql
