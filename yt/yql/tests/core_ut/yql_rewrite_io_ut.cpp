#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_opt_rewrite_io.h>
#include <yql/essentials/core/yql_opt_proposed_by_data.h>

#include <yt/yql/providers/yt/provider/yql_yt_provider.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file.h>
#include <yt/yql/providers/yt/gateway/file/yql_yt_file_services.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/ast/yql_ast_annotation.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/type_ann/type_ann_core.h>
#include <yql/essentials/core/type_ann/type_ann_expr.h>
#include <yt/yql/providers/yt/lib/ut_common/yql_ut_common.h>
#include <yql/essentials/core/facade/yql_facade.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {

Y_UNIT_TEST_SUITE(TYtOptimizeYqlExpr) {
    Y_UNIT_TEST(CheckIORewrite) {
        auto s = "(\n"
            "(let mr_source (DataSource 'yt 'plato))\n"
            "(let x (Read! world mr_source (Key '('table (String 'Input))) '('key 'subkey 'value) '()))\n"
            "(let world (Left! x))\n"
            "(let table1 (Right! x))\n"
            "(let tresh (String '100))\n"
            "(let table1low (Filter table1 (lambda '(item1) (> tresh (Member item1 'key)))))\n"
            "(let mr_sink (DataSink 'yt (quote plato)))\n"
            "(let world (Write! world mr_sink (Key '('table (String 'Output))) table1low '('('mode 'append))))\n"
            "(let world (Commit! world mr_sink))\n"
            "(return world)\n"
            ")\n";

        TAstParseResult astRes = ParseAst(s);
        UNIT_ASSERT(astRes.IsOk());
        TExprContext exprCtx;
        TExprNode::TPtr exprRoot;
        UNIT_ASSERT(CompileExpr(*astRes.Root, exprRoot, exprCtx, nullptr, nullptr));

        auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        TTestTablesMapping testTables;
        auto yqlNativeServices = NFile::TYtFileServices::Make(functionRegistry.Get(), testTables);
        auto ytGateway = CreateYtFileGateway(yqlNativeServices);
        auto typeAnnotationContext = MakeIntrusive<TTypeAnnotationContext>();
        auto ytState = MakeIntrusive<TYtState>(typeAnnotationContext.Get());
        ytState->Gateway = ytGateway;

        InitializeYtGateway(ytGateway, ytState);
        auto randomProvider = CreateDeterministicRandomProvider(1);
        typeAnnotationContext->AddDataSink(YtProviderName, CreateYtDataSink(ytState));
        typeAnnotationContext->AddDataSource(YtProviderName, CreateYtDataSource(ytState));
        typeAnnotationContext->RandomProvider = randomProvider;
        auto intentTransformer = CreateIntentDeterminationTransformer(*typeAnnotationContext);
        TVector<TTransformStage> transformers;
        const auto issueCode = TIssuesIds::DEFAULT_ERROR;
        transformers.push_back(TTransformStage(
            CreateIODiscoveryTransformer(*typeAnnotationContext),
            "IODiscovery",
            issueCode));
        transformers.push_back(TTransformStage(
            CreateEpochsTransformer(*typeAnnotationContext),
            "Epochs",
            issueCode));

        // NOTE: add fake EvaluateExpression step to break infinite loop
        // (created by Repeat on ExprEval step after RewriteIO completion)
        transformers.push_back(TTransformStage(
            CreateFunctorTransformer(
                [](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
                    output = input;
                    ctx.Step.Done(TExprStep::ExprEval);
                    return IGraphTransformer::TStatus::Ok;
                }
            ),
            "EvaluateExpression",
            issueCode));

        transformers.push_back(TTransformStage(
            intentTransformer,
            "IntentDetermination",
            issueCode));
        transformers.push_back(TTransformStage(
            CreateTableMetadataLoader(*typeAnnotationContext),
            "MetadataLoader",
            issueCode));
        // NOTE: metadata loader unconditionally drops ExpandApplyForLambdas flag
        // in DoApplyAsyncChanges, so ExpandApply transformation is in such an unusual place
        transformers.push_back(TTransformStage(
            CreateFunctorTransformer(&ExpandApply),
            "ExpandApply",
            issueCode));
        auto fullTransformer = CreateCompositeGraphTransformer(transformers, true);
        bool success = SyncTransform(*fullTransformer, exprRoot, exprCtx) == IGraphTransformer::TStatus::Ok;
        UNIT_ASSERT(success);

        UNIT_ASSERT(RewriteIO(exprRoot, exprRoot, *typeAnnotationContext, exprCtx).Level == IGraphTransformer::TStatus::Ok);

        auto ast = ConvertToAst(*exprRoot, exprCtx, TExprAnnotationFlags::None, true);
        auto strRes = ast.Root->ToString(TAstPrintFlags::PerLine | TAstPrintFlags::ShortQuote);
        UNIT_ASSERT(strRes.find("(YtReadTable!") != TString::npos);
        UNIT_ASSERT(strRes.find("(Read!") == TString::npos);
    }
}

} // namespace NYql
