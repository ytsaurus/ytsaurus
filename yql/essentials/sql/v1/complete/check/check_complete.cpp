#include "check_complete.h"

#include "collect_clusters.h"
#include "collect_tables.h"

#include <yql/essentials/sql/v1/complete/sql_complete.h>
#include <yql/essentials/sql/v1/complete/name/cluster/static/discovery.h>
#include <yql/essentials/sql/v1/complete/name/object/simple/static/schema.h>
#include <yql/essentials/sql/v1/complete/name/service/cluster/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/schema/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/static/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/union/name_service.h>

#include <yql/essentials/ast/yql_expr.h>

#define USE_CURRENT_UDF_ABI_VERSION
#include <yql/essentials/core/services/yql_transform_pipeline.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/services/yql_eval_expr.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/providers/result/provider/yql_result_provider.h>

#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

#include <util/charset/utf8.h>
#include <util/random/random.h>

namespace NSQLComplete {

    namespace {

        TLexerSupplier MakePureLexerSupplier() {
            NSQLTranslationV1::TLexers lexers;
            lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
            lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();
            return [lexers = std::move(lexers)](bool ansi) {
                return NSQLTranslationV1::MakeLexer(
                    lexers, ansi, /* antlr4 = */ true,
                    NSQLTranslationV1::ELexerFlavor::Pure);
            };
        }

        INameService::TPtr MakeClusterNameService(NYql::TExprNode& expr) {
            THashSet<TString> clusterSet = CollectClusters(expr);

            TVector<TString> clusterVec(begin(clusterSet), std::end(clusterSet));
            Sort(clusterVec);

            Cerr << "[complete] " << "Cluster List" << Endl;
            for (const auto& cluster : clusterVec) {
                Cerr << "[complete] " << "  " << cluster << Endl;
            }

            auto discovery = MakeStaticClusterDiscovery(std::move(clusterVec));

            return MakeClusterNameService(std::move(discovery));
        }

        INameService::TPtr MakeSchemaNameService(NYql::TExprNode& expr) {
            THashMap<TString, THashMap<TString, TVector<TFolderEntry>>> fs = CollectTables(expr);

            Cerr << "[complete] " << "TablesByCluster Mapping" << Endl;
            for (const auto& [cluster, tree] : fs) {
                Cerr << "[complete] " << "  " << cluster << ":" << Endl;
                for (const auto& [path, entries] : tree) {
                    Cerr << "[complete] " << "    " << path << ":" << Endl;
                    for (const auto& entry : entries) {
                        Cerr << "[complete] " << "      " << entry << Endl;
                    }
                }
            }

            return MakeSchemaNameService(MakeSimpleSchema(MakeStaticSimpleSchema(std::move(fs))));
        }

    } // namespace

    bool CheckComplete(TStringBuf query, NYql::TAstNode& root, TString& error) try {
        constexpr size_t Seed = 97651231;
        constexpr size_t Attempts = 64;
        constexpr size_t MaxAttempts = 256;
        SetRandomSeed(Seed);

        NYql::TExprContext ctx;
        NYql::TExprNode::TPtr expr;
        if (!NYql::CompileExpr(
                root, expr, ctx,
                /* resolver = */ nullptr,
                /* urlListerManager = */ nullptr)) {
            error = ctx.IssueManager.GetIssues().ToOneLineString();
            return false;
        }

        auto builtins = NKikimr::NMiniKQL::CreateBuiltinRegistry();
        auto functionRegistry = NKikimr::NMiniKQL::CreateFunctionRegistry(std::move(builtins));

        auto types = MakeIntrusive<NYql::TTypeAnnotationContext>();

        for (size_t i = 0; i < 128; ++i) {
            auto status = NYql::EvaluateExpression(expr, expr, *types, ctx, *functionRegistry);
            if (status.Level != NYql::IGraphTransformer::TStatus::Repeat) {
                Y_ENSURE(status == NYql::IGraphTransformer::TStatus::Ok, "" << status);
                break;
            }
        }

        ConvertToAst(*expr, ctx, NYql::TExprAnnotationFlags::None, true).Root->PrettyPrintTo(Cerr, NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote);

        auto service = MakeUnionNameService(
            {
                MakeClusterNameService(*expr),
                MakeSchemaNameService(*expr),
            },
            MakeDefaultRanking());

        auto engine = MakeSqlCompletionEngine(MakePureLexerSupplier(), std::move(service));

        for (size_t i = 0, j = 0; i < Attempts && j < MaxAttempts; ++j) {
            size_t pos = RandomNumber<size_t>(query.size() + 1);
            if (pos < query.size() && IsUTF8ContinuationByte(query.at(pos))) {
                continue;
            }

            TCompletionInput input = {
                .Text = query,
                .CursorPosition = pos,
            };

            auto output = engine->CompleteAsync(input).ExtractValueSync();
            Y_DO_NOT_OPTIMIZE_AWAY(output);

            i += 1;
        }

        return true;
    } catch (...) {
        error = CurrentExceptionMessage();
        return false;
    }

} // namespace NSQLComplete
