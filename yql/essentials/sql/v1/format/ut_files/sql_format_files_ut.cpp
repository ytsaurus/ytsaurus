#include <yql/essentials/sql/v1/format/sql_format.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/arena.h>
#include <util/folder/path.h>
#include <util/generic/algorithm.h>
#include <util/stream/file.h>

namespace {

constexpr TStringBuf QuerySuffix = ".yql";
constexpr TStringBuf FormattedSuffix = ".fmt.yql";

void CheckCase(const TFsPath& sourcePath, const TFsPath& expectedPath) {
    const TString source = TFileInput(sourcePath.GetPath()).ReadAll();
    const TString expected = TFileInput(expectedPath.GetPath()).ReadAll();

    const NSQLTranslationV1::TLexers lexers = {
        .Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory(),
        .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory(),
    };
    const NSQLTranslationV1::TParsers parsers = {
        .Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(),
        .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(),
    };
    google::protobuf::Arena arena;
    NSQLTranslation::TTranslationSettings settings;
    settings.Arena = &arena;
    auto formatter = NSQLFormat::MakeSqlFormatter(lexers, parsers, settings);

    NYql::TIssues issues;
    TString formatted;
    UNIT_ASSERT_C(formatter->Format(source, formatted, issues), sourcePath.GetPath() << ": " << issues.ToString());
    UNIT_ASSERT_C(formatted == expected, sourcePath.GetPath() << "\n"
                                                              << NUnitTest::ColoredDiff(expected, formatted));

    TString reformatted;
    UNIT_ASSERT_C(formatter->Format(formatted, reformatted, issues), sourcePath.GetPath() << ": " << issues.ToString());
    UNIT_ASSERT_C(reformatted == formatted, sourcePath.GetPath() << ": formatting twice changed the output\n"
                                                                 << NUnitTest::ColoredDiff(formatted, reformatted));

    const auto mutated = NSQLFormat::MutateQuery(lexers, source, settings);
    UNIT_ASSERT_C(formatter->Format(mutated, reformatted, issues), sourcePath.GetPath() << ": " << issues.ToString());
}

} // namespace

Y_UNIT_TEST_SUITE(CheckSqlFormatterFiles) {

Y_UNIT_TEST(Pretty) {
    const auto data = TFsPath(ArcadiaSourceRoot()) / "yql/essentials/sql/v1/format/ut_files/data";
    TVector<TString> suites;
    data.ListNames(suites);
    Sort(suites);

    size_t caseCount = 0;
    for (const auto& suite : suites) {
        const auto suitePath = data / suite;
        if (!suitePath.IsDirectory()) {
            continue;
        }

        TVector<TString> files;
        suitePath.ListNames(files);
        Sort(files);
        for (const auto& name : files) {
            const auto path = suitePath / name;
            if (!name.EndsWith(QuerySuffix) || !path.IsFile()) {
                continue;
            }
            if (name.EndsWith(FormattedSuffix)) {
                const auto source = suitePath / (name.substr(0, name.size() - FormattedSuffix.size()) + ".yql");
                UNIT_ASSERT_C(source.IsFile(), "Missing input for " << path.GetPath());
                continue;
            }

            const auto expected = suitePath / (name.substr(0, name.size() - QuerySuffix.size()) + ".fmt.yql");
            UNIT_ASSERT_C(expected.IsFile(), "Missing expected output for " << path.GetPath() << ": " << expected.GetPath());
            CheckCase(path, expected);
            ++caseCount;
        }
    }
    UNIT_ASSERT_C(caseCount > 0, "No formatter cases found in " << data.GetPath());
}

} // Y_UNIT_TEST_SUITE(CheckSqlFormatterFiles)
