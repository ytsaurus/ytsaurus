#include "sql_reflect.h"

#include <library/cpp/resource/resource.h>

#include <util/string/split.h>
#include <util/string/strip.h>

// TODO(vityaman): remove before a code review.
// #include <iostream>

namespace NSQLReflect {
    namespace {
        const TStringBuf ReflectPrefix = "//!";
        const TStringBuf SectionPrefix = "//! section:";
        const TStringBuf SectionPunctuation = "//! section:punctuation";
        const TStringBuf SectionLetter = "//! section:letter";
        const TStringBuf SectionKeyword = "//! section:keyword";
        const TStringBuf SectionOther = "//! section:other";
        const TStringBuf SubstPrefix = "//! subst:";
        const TStringBuf FragmentPrefix = "fragment ";

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

        THashMap<TString, THashMap<TString, TString>> ExtractSubstitutions(TVector<TString>& lines) {
            THashMap<TString, THashMap<TString, TString>> substitutions;

            for (auto& line : lines) {
                if (!line.StartsWith(SubstPrefix)) {
                    continue;
                }

                SubstGlobal(line, SubstPrefix, "");
                SubstGlobal(line, " GRAMMAR", "$$GRAMMAR");
                SubstGlobal(line, " = ", "$$");

                TVector<TString> parts;
                Split(line, "$$", parts);
                Y_ENSURE(parts.size() == 3);

                substitutions[std::move(parts[0])][std::move(parts[1])] = std::move(parts[2]);
            }

            const auto [first, last] = std::ranges::remove_if(lines, [](const TString& line) {
                return line.Contains("$$");
            });
            lines.erase(first, last);

            return substitutions;
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

        void ParsePunctuationLine(TString&& line, TGrammarMeta& meta) {
            auto [name, content] = ParseLexerRule(std::move(line));
            content = content.erase(std::begin(content));
            content.pop_back();

            SubstGlobal(content, "\\\'", "\'");

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
            SubstGlobal(content, " -> channel(HIDDEN)", "");
            meta.ContentByName.emplace(std::move(name), std::move(content));
        }
    } // namespace

    TGrammarMeta GetGrammarMeta() {
        TVector<TString> lines = GetResourceLines("SQLv1Antlr4.g.in");
        Purify(lines);
        Format(lines);
        Purify(lines);

        auto substitutions = ExtractSubstitutions(lines);

        THashMap<TStringBuf, TVector<TString>> sections;
        sections = GroupBySection(std::move(lines));

        TGrammarMeta meta;

        meta.Substitutions = std::move(substitutions);

        for (auto& [section, lines] : sections) {
            for (auto& line : lines) {
                if (section == SectionPunctuation) {
                    ParsePunctuationLine(std::move(line), meta);
                } else if (section == SectionKeyword) {
                    ParseKeywordLine(std::move(line), meta);
                } else if (section == SectionOther) {
                    ParseOtherLine(std::move(line), meta);
                } else {
                    Y_ABORT("Unexpected section %s", section);
                }
            }
        }

        // TODO(vityaman): remove before a code review.
        // std::cout << ">> Remaining content <<" << std::endl;
        // for (auto& [section, lines] : sections) {
        //     std::cout << ">> Section " << section << std::endl;
        //     for (auto& line : lines) {
        //         std::cout << line << std::endl;
        //     }
        // }
        // std::cout << std::endl;
        // std::cout << ">> Meta <<" << std::endl;
        // std::cout << "Keywords:" << std::endl;
        // for (const auto& token : meta.Keywords) {
        //     std::cout << "- " << token << std::endl;
        // }
        // std::cout << "Punctuation:" << std::endl;
        // for (const auto& token : meta.Punctuation) {
        //     std::cout << "- " << token << ": " << meta.ContentByName.at(token) << std::endl;
        // }
        // std::cout << "Other:" << std::endl;
        // for (const auto& token : meta.Other) {
        //     std::cout << "- " << token << ": " << meta.ContentByName.at(token) << std::endl;
        // }
        // std::cout << "Fragment:" << std::endl;
        // for (const auto& [name, content] : meta.ContentByName) {
        //     if (meta.Punctuation.contains(name) || meta.Other.contains(name)) {
        //         continue;
        //     }
        //     std::cout << "- " << name << ": " << content << std::endl;
        // }
        // std::cout << "Substitutions:" << std::endl;
        // for (const auto& [mode, dict] : meta.Substitutions) {
        //     std::cout << "- " << mode << std::endl;
        //     for (const auto& [k, v] : dict) {
        //         std::cout << "  - " << k << ": " << v << std::endl;
        //     }
        // }
        // std::cout << std::endl;

        return meta;
    }

} // namespace NSQLReflect
