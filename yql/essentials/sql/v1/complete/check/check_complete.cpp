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

        TNameSet MakeNameSet() {
            return {
                .Pragmas = {
                    "yson.CastToString",
                    "yt.RuntimeCluster",
                    "yt.RuntimeClusterSelection",
                },
                .Types = {"Uint64"},
                .Functions = {
                    "StartsWith",
                    "DateTime::Split",
                    "Python::__private",
                },
                .Hints = {
                    {EStatementKind::Select, {"XLOCK"}},
                    {EStatementKind::Insert, {"EXPIRATION"}},
                },
            };
        }

        THashMap<TString, THashMap<TString, TVector<TFolderEntry>>> MakeObjectTree() {
            return {
                {"", {{"/", {{"Folder", "local"},
                             {"Folder", "test"},
                             {"Folder", "prod"},
                             {"Folder", ".sys"}}},
                      {"/local/", {{"Table", "example"},
                                   {"Table", "account"},
                                   {"Table", "abacaba"}}},
                      {"/test/", {{"Folder", "service"},
                                  {"Table", "meta"}}},
                      {"/test/service/", {{"Table", "example"}}},
                      {"/.sys/", {{"Table", "status"}}}}},
                {"example",
                 {{"/", {{"Table", "people"},
                         {"Folder", "yql"}}},
                  {"/yql/", {{"Table", "tutorial"}}}}},
                {"yt:saurus",
                 {{"/", {{"Table", "maxim"}}}}},
            };
        }

        ISqlCompletionEngine::TPtr MakeSqlCompletionEngineUT() {
            TLexerSupplier lexer = MakePureLexerSupplier();

            auto names = MakeNameSet();
            auto tree = MakeObjectTree();

            TVector<TString> clusters;
            for (const auto& [cluster, _] : tree) {
                clusters.emplace_back(cluster);
            }
            EraseIf(clusters, [](const auto& s) { return s.empty(); });

            THashMap<TString, ISchema::TPtr> schemasByCluster;
            for (auto& [cluster, fs] : tree) {
                schemasByCluster[std::move(cluster)] =
                    MakeSimpleSchema(
                        MakeStaticSimpleSchema(std::move(fs)));
            }

            TFrequencyData frequency;
            IRanking::TPtr ranking = MakeDefaultRanking(frequency);

            TVector<INameService::TPtr> children = {
                MakeStaticNameService(std::move(names), frequency),
                MakeSchemaNameService(MakeDispatchSchema(std::move(schemasByCluster))),
                MakeClusterNameService(MakeStaticClusterDiscovery(std::move(clusters))),
            };

            INameService::TPtr service = MakeUnionNameService(std::move(children), ranking);

            return MakeSqlCompletionEngine(
                std::move(lexer), std::move(service));
        }

    } // namespace

    bool CheckComplete(TStringBuf query, TString& error) try {
        constexpr size_t Seed = 97651231;
        constexpr size_t Attempts = 8;

        SetRandomSeed(Seed);

        auto engine = MakeSqlCompletionEngineUT();

        for (size_t i = 0; i < Attempts;) {
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
