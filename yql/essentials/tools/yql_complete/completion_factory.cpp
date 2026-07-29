#include "completion_factory.h"

#include <yql/essentials/sql/v1/ide/completion/name/cluster/static/discovery.h>
#include <yql/essentials/sql/v1/ide/completion/name/object/simple/static/schema_json.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/cluster/name_service.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/ranking/ranking.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/schema/name_service.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/static/name_service.h>
#include <yql/essentials/sql/v1/ide/completion/name/service/union/name_service.h>

#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

#include <library/cpp/iterator/functools.h>
#include <library/cpp/iterator/iterate_keys.h>

#include <util/generic/vector.h>

namespace {

NSQLComplete::TLexerSupplier MakePureLexerSupplier() {
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
    lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();
    return [lexers = std::move(lexers)](bool ansi) {
        return NSQLTranslationV1::MakeLexer(
            lexers, ansi, NSQLTranslationV1::ELexerFlavor::Pure);
    };
}

} // namespace

TCompletionFactory::TCompletionFactory(NSQLComplete::TFrequencyData frequency)
    : Ranking_(NSQLComplete::MakeDefaultRanking(frequency))
    , StaticNames_(NSQLComplete::MakeStaticNameService(
        NSQLComplete::LoadDefaultNameSet(),
        Ranking_))
    , Lexer_(MakePureLexerSupplier())
{
}

NSQLComplete::ISqlCompletionEngine::TPtr TCompletionFactory::MakeEngine(
    const NJson::TJsonValue* schemaValue) const
{
    TVector<NSQLComplete::INameService::TPtr> services = {StaticNames_};

    if (schemaValue) {
        if (!schemaValue->IsMap()) {
            ythrow yexception() << "schema must be a map";
        }

        NJson::TJsonMap schema;
        schema.GetMapSafe() = schemaValue->GetMapSafe();

        services.emplace_back(
            NSQLComplete::MakeSchemaNameService(
                NSQLComplete::MakeSimpleSchema(
                    NSQLComplete::MakeStaticSimpleSchema(schema))));

        auto clustersIt = NFuncTools::Filter(
            [](const auto& x) { return !x.empty(); },
            IterateKeys(schema.GetMapSafe()));
        TVector<TString> clusters(clustersIt.begin(), clustersIt.end());

        services.emplace_back(
            NSQLComplete::MakeClusterNameService(
                NSQLComplete::MakeStaticClusterDiscovery(std::move(clusters))));
    }

    return NSQLComplete::MakeSqlCompletionEngine(
        Lexer_,
        NSQLComplete::MakeUnionNameService(std::move(services), Ranking_));
}
