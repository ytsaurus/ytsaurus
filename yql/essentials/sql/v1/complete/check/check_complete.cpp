#include "check_complete.h"

#include <yql/essentials/sql/v1/complete/sql_complete.h>
#include <yql/essentials/sql/v1/complete/name/cluster/static/discovery.h>
#include <yql/essentials/sql/v1/complete/name/object/dispatch/schema.h>
#include <yql/essentials/sql/v1/complete/name/object/simple/static/schema.h>
#include <yql/essentials/sql/v1/complete/name/service/cluster/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/schema/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/static/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/union/name_service.h>

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

        ISqlCompletionEngine::TPtr MakeSqlCompletionEngineUT(
            THashMap<TString, TVector<TString>> tablesByCluster) {
            THashMap<TString, THashMap<TString, TVector<TFolderEntry>>> tree;
            for (auto&& [cluster, tables] : tablesByCluster) {
                for (auto&& table : tables) {
                    tree[cluster]["/"].push_back({
                        .Type = TFolderEntry::Table,
                        .Name = std::move(table),
                    });
                }
            }

            TVector<TString> clusters;
            for (const auto& [cluster, _] : tree) {
                clusters.emplace_back(cluster);
            }

            Y_ENSURE(!tree.empty());
            tree[""]["/"] = std::begin(tree)->second["/"];

            THashMap<TString, ISchema::TPtr> schemasByCluster;
            for (auto& [cluster, fs] : tree) {
                schemasByCluster[std::move(cluster)] =
                    MakeSimpleSchema(
                        MakeStaticSimpleSchema(std::move(fs)));
            }

            TFrequencyData frequency;
            IRanking::TPtr ranking = MakeDefaultRanking(frequency);

            TVector<INameService::TPtr> children = {
                MakeStaticNameService(MakeDefaultNameSet(), frequency),
                MakeSchemaNameService(MakeDispatchSchema(std::move(schemasByCluster))),
                MakeClusterNameService(MakeStaticClusterDiscovery(std::move(clusters))),
            };

            INameService::TPtr service = MakeUnionNameService(std::move(children), ranking);

            return MakeSqlCompletionEngine(MakePureLexerSupplier(), std::move(service));
        }

    } // namespace

    bool CheckComplete(
        TStringBuf query,
        THashMap<TString, TVector<TString>> tablesByCluster,
        TString& error) try {
        constexpr size_t Seed = 97651231;
        constexpr size_t Attempts = 64;
        constexpr size_t MaxAttempts = 256;

        SetRandomSeed(Seed);

        auto engine = MakeSqlCompletionEngineUT(std::move(tablesByCluster));

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
