#include "sql_reflect.h"

#include <library/cpp/resource/resource.h>

#include <util/string/split.h>
#include <util/string/strip.h>

namespace NSQLReflect {

    const TStringBuf ReflectPrefix = "//!";
    const TStringBuf SectionPrefix = "//! section:";
    const TStringBuf SectionPunctuation = "//! section:punctuation";
    const TStringBuf SectionLetter = "//! section:letter";
    const TStringBuf SectionKeyword = "//! section:keyword";
    const TStringBuf SectionOther = "//! section:other";
    const TStringBuf FragmentPrefix = "fragment ";

    class TLexerGrammar: public ILexerGrammar {
    public:
        const THashSet<TString>& GetKeywords() const override {
            return Keywords;
        }

        const THashSet<TString>& GetPunctuation() const override {
            return Punctuation;
        }

        const THashSet<TString>& GetOther() const override {
            return Other;
        }

        const TString& GetContentByName(const TString& name) const override {
            auto it = ContentByName.find(name);
            if (it != ContentByName.end()) {
                return it->second;
            }
            Y_ENSURE(GetKeywords().contains(name));
            return name;
        }

        THashSet<TString> Keywords;
        THashSet<TString> Punctuation;
        THashSet<TString> Other;
        THashMap<TString, TString> ContentByName;
    };

    TVector<TString> GetResourceLines(const TStringBuf key) {
        TString text;
        Y_ENSURE(NResource::FindExact(key, &text));

        TVector<TString> lines;
        Split(text, "\n", lines);
        return lines;
    }

    void Format(TVector<TString>& lines) {
        for (size_t i = 0; i < lines.size(); ++i) {
            auto& line = lines[i];

            StripInPlace(line);

            if (line.StartsWith("//") || (line.Contains(':') && line.Contains(';'))) {
                continue;
            }

            size_t j = i + 1;
            do {
                line += lines.at(j);
            } while (!lines.at(j++).Contains(';'));

            auto first = std::next(std::begin(lines), i + 1);
            auto last = std::next(std::begin(lines), j);
            lines.erase(first, last);
        }

        for (auto& line : lines) {
            CollapseInPlace(line);
            SubstGlobal(line, " ;", ";");
            SubstGlobal(line, " :", ":");
            SubstGlobal(line, " )", ")");
            SubstGlobal(line, "( ", "(");
        }
    }

    void Purify(TVector<TString>& lines) {
        const auto [first, last] = std::ranges::remove_if(lines, [](const TString& line) {
            return (line.StartsWith("//") && !line.StartsWith(ReflectPrefix)) || line.empty();
        });
        lines.erase(first, last);
    }

    THashMap<TStringBuf, TVector<TString>> GroupBySection(TVector<TString>&& lines) {
        TVector<TStringBuf> sections = {
            "",
            SectionPunctuation,
            SectionLetter,
            SectionKeyword,
            SectionOther,
        };

        size_t section = 0;

        THashMap<TStringBuf, TVector<TString>> groups;
        for (auto& line : lines) {
            if (line.StartsWith(SectionPrefix)) {
                Y_ENSURE(sections.at(section + 1) == line);
                section += 1;
                continue;
            }

            groups[sections.at(section)].emplace_back(std::move(line));
        }

        groups.erase("");
        groups.erase(SectionLetter);

        return groups;
    }

    std::tuple<TString, TString> ParseLexerRule(TString&& line) {
        size_t colonPos = line.find(':');
        size_t semiPos = line.rfind(';');

        Y_ENSURE(
            colonPos != TString::npos &&
            semiPos != TString::npos &&
            colonPos < semiPos);

        TString content = line.substr(colonPos + 2, semiPos - colonPos - 2);
        SubstGlobal(content, "\\\\", "\\");

        TString name = std::move(line);
        name.resize(colonPos);

        return std::make_tuple(std::move(name), std::move(content));
    }

    void ParsePunctuationLine(TString&& line, TLexerGrammar& grammar) {
        auto [name, content] = ParseLexerRule(std::move(line));
        content = content.erase(std::begin(content));
        content.pop_back();

        SubstGlobal(content, "\\\'", "\'");

        if (!name.StartsWith(FragmentPrefix)) {
            grammar.Punctuation.emplace(name);
        }

        SubstGlobal(name, FragmentPrefix, "");
        grammar.ContentByName.emplace(std::move(name), std::move(content));
    }

    void ParseKeywordLine(TString&& line, TLexerGrammar& grammar) {
        auto [name, content] = ParseLexerRule(std::move(line));
        SubstGlobal(content, "'", "");
        SubstGlobal(content, " ", "");

        Y_ENSURE(name == content);
        grammar.Keywords.emplace(std::move(name));
    }

    void ParseOtherLine(TString&& line, TLexerGrammar& grammar) {
        auto [name, content] = ParseLexerRule(std::move(line));

        if (!name.StartsWith(FragmentPrefix)) {
            grammar.Other.emplace(name);
        }

        SubstGlobal(name, FragmentPrefix, "");
        SubstGlobal(content, " -> channel(HIDDEN)", "");
        grammar.ContentByName.emplace(std::move(name), std::move(content));
    }

    ILexerGrammar::TPtr LoadLexerGrammar() {
        TVector<TString> lines = GetResourceLines("SQLv1Antlr4.g.in");
        Purify(lines);
        Format(lines);
        Purify(lines);

        THashMap<TStringBuf, TVector<TString>> sections;
        sections = GroupBySection(std::move(lines));

        auto grammar = MakeHolder<TLexerGrammar>();

        for (auto& [section, lines] : sections) {
            for (auto& line : lines) {
                if (section == SectionPunctuation) {
                    ParsePunctuationLine(std::move(line), *grammar);
                } else if (section == SectionKeyword) {
                    ParseKeywordLine(std::move(line), *grammar);
                } else if (section == SectionOther) {
                    ParseOtherLine(std::move(line), *grammar);
                } else {
                    Y_ABORT("Unexpected section %s", section);
                }
            }
        }

        return grammar;
    }

} // namespace NSQLReflect
