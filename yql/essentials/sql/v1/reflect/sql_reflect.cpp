#include "sql_reflect.h"

#include <library/cpp/resource/resource.h>

#include <util/string/split.h>
#include <util/string/strip.h>

#include <iostream>

namespace NSQLReflect {
    namespace {
        const TStringBuf ReflectPrefix = "//!";
        const TStringBuf ReflectPunctuation = "//! reflect:punctuation";
        const TStringBuf ReflectLetter = "//! reflect:letter";
        const TStringBuf ReflectKeyword = "//! reflect:keyword";
        const TStringBuf ReflectOther = "//! reflect:other";
        const TStringBuf FragmentPrefix = "fragment ";

        TVector<TString> GetResourceLines(const TStringBuf key) {
            TString text;
            Y_ENSURE(NResource::FindExact(key, &text));

            TVector<TString> lines;
            Split(text, "\n", lines);
            return lines;
        }

        TVector<TString> Format(TVector<TString>&& lines) {
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

            return lines;
        }

        TVector<TString> Purify(TVector<TString>&& lines) {
            const auto [first, last] = std::ranges::remove_if(lines, [](const TString& line) {
                return (line.StartsWith("//") && !line.StartsWith(ReflectPrefix)) || line.empty();
            });
            lines.erase(first, last);
            return lines;
        }

        THashMap<TStringBuf, TVector<TString>> GroupBySection(TVector<TString>&& lines) {
            TVector<TStringBuf> sections = {
                "",
                ReflectPunctuation,
                ReflectLetter,
                ReflectKeyword,
                ReflectOther,
            };

            size_t section = 0;

            THashMap<TStringBuf, TVector<TString>> groups;
            for (auto& line : lines) {
                if (line.StartsWith(ReflectPrefix)) {
                    Y_ENSURE(sections.at(section + 1) == line);
                    section += 1;
                    continue;
                }

                groups[sections.at(section)].emplace_back(std::move(line));
            }

            groups.erase("");
            groups.erase(ReflectLetter);

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
    
            TString name = std::move(line);
            name.resize(colonPos);
    
            return std::make_tuple(std::move(name), std::move(content));
        }
    
        void ParsePunctuationLine(TString&& line, TGrammarMeta& meta) {
            auto [name, content] = ParseLexerRule(std::move(line));
            SubstGlobal(content, "'", "");
    
            if (!name.StartsWith(FragmentPrefix)) {
                meta.Punctuation.emplace(name);
            }
    
            SubstGlobal(name, FragmentPrefix, "");
            meta.ContentByName.emplace(std::move(name), std::move(content));
        }
    
        void ParseKeywordLine(TString&& line, TGrammarMeta& meta) {
            auto [name, content] = ParseLexerRule(std::move(line));
            SubstGlobal(content, "'", "");
            SubstGlobal(content, " ", "");
    
            Y_ENSURE(name == content);
            meta.Keywords.emplace(std::move(name));
        }
    
        void ParseOtherLine(TString&& line, TGrammarMeta& meta) {
            auto [name, content] = ParseLexerRule(std::move(line));
    
            if (!name.StartsWith(FragmentPrefix)) {
                meta.Other.emplace(name);
            }
    
            SubstGlobal(name, FragmentPrefix, "");
            meta.ContentByName.emplace(std::move(name), std::move(content));
        }
    } // namespace

    TGrammarMeta GetGrammarMeta() {
        TVector<TString> lines;
        lines = GetResourceLines("SQLv1Antlr4.g.in");
        lines = Purify(std::move(lines));
        lines = Format(std::move(lines));
        lines = Purify(std::move(lines));

        THashMap<TStringBuf, TVector<TString>> sections;
        sections = GroupBySection(std::move(lines));

        TGrammarMeta meta;

        for (auto& [section, lines] : sections) {
            for (auto& line : lines) {
                if (section == ReflectPunctuation) {
                    ParsePunctuationLine(std::move(line), meta);
                } else if (section == ReflectKeyword) {
                    ParseKeywordLine(std::move(line), meta);
                } else if (section == ReflectOther) {
                    ParseOtherLine(std::move(line), meta);
                } else {
                    Y_ABORT("Unexpected section %s", section);
                }
            }
        }

        std::cout << ">> Remaining content <<" << std::endl;
        for (auto& [section, lines] : sections) {
            std::cout << ">> Section " << section << std::endl;
            for (auto& line : lines) {
                std::cout << line << std::endl;
            }
        }
        std::cout << std::endl;

        std::cout << ">> Meta <<" << std::endl;
        std::cout << "Keywords:" << std::endl;
        for (const auto& token : meta.Keywords) {
            std::cout << "- " << token << std::endl;
        }
        std::cout << "Punctuation:" << std::endl;
        for (const auto& token : meta.Punctuation) {
            std::cout << "- " << token << ": " << meta.ContentByName.at(token) << std::endl;
        }
        std::cout << "Other:" << std::endl;
        for (const auto& token : meta.Other) {
            std::cout << "- " << token << ": " << meta.ContentByName.at(token) << std::endl;
        }
        std::cout << "Fragment:" << std::endl;
        for (const auto& [name, content] : meta.ContentByName) {
            if (meta.Punctuation.contains(name) || meta.Other.contains(name)) {
                continue;
            }
            std::cout << "- " << name << ": " << content << std::endl;
        }
        std::cout << std::endl;

        return meta;
    }

} // namespace NSQLReflect
