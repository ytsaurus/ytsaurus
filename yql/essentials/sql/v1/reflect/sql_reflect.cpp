#include "sql_reflect.h"

#include <library/cpp/resource/resource.h>

#include <util/string/split.h>
#include <util/string/strip.h>

namespace NSQLReflect {

    class TGrammarMetaParser {
    public:
        explicit TGrammarMetaParser(TString text) {
            Split(text, "\n", Lines_);
            for (auto& line : Lines_) {
                StripInPlace(line);
                CollapseInPlace(line);
            }

            CollectFragments();

            Line_ = std::begin(Lines_);

            ParsePrefix();
            ParsePunctuation();
            ParseCaseInsensitiveLetter();
            ParseKeyword();
            ParseOther();
        }

        TGrammarMeta&& ExtractMeta() && {
            return std::move(Meta_);
        }

    private:
        bool ParseTrivialLexerRule(TString&& line, TString& name, TString& content) {
            size_t colonPos = line.find(':');
            size_t semiPos = line.find(';');

            if (colonPos == TString::npos || semiPos == TString::npos ||
                semiPos < colonPos + 2) {
                return false;
            }

            content = line.substr(colonPos + 1, semiPos - colonPos - 1);
            StripInPlace(content);

            name = std::move(line);
            name.resize(colonPos);
            StripInPlace(name);

            return true;
        }

        void CollectFragments() {
            for (auto& line : Lines_) {
                if (!line.StartsWith("fragment ")) {
                    continue;
                }
                line.erase(0, sizeof("fragment ") - 1);

                TString name, content;
                Y_ENSURE(ParseTrivialLexerRule(std::move(line), name, content));

                Meta_.ContentByName.emplace(std::move(name), std::move(content));
            }
        }

        void ParsePrefix() {
            for (; !Line_->StartsWith("//!"); ++Line_) {
                // Skip
            }
        }

        void ParsePunctuation() {
            Y_ENSURE(*(Line_++) == "//! reflect:punctuation");
            for (; !Line_->StartsWith("//!"); ++Line_) {
                if (Line_->empty() || Line_->StartsWith("//")) {
                    continue;
                }

                TString name, content;
                Y_ENSURE(ParseTrivialLexerRule(std::move(*Line_), name, content));

                SubstGlobal(content, " ", "");
                SubstGlobal(content, "'", "");

                Meta_.ContentByName[name] = std::move(content);
                Meta_.Punctuation.emplace(std::move(name));
            }
        }

        void ParseCaseInsensitiveLetter() {
            Y_ENSURE(*(Line_++) == "//! reflect:case-insensitive-letter");
            for (; !Line_->StartsWith("//!"); ++Line_) {
                // Skip
            }
        }

        void ParseKeyword() {
            Y_ENSURE(*(Line_++) == "//! reflect:keyword");
            for (; !Line_->StartsWith("//!"); ++Line_) {
                if (Line_->empty() || Line_->StartsWith("//")) {
                    continue;
                }

                TString name, content;
                Y_ENSURE(ParseTrivialLexerRule(std::move(*Line_), name, content));

                SubstGlobal(content, " ", "");
                SubstGlobal(content, "'", "");

                Y_ENSURE(name == content);
                Meta_.Keywords.emplace(std::move(name));
            }
        }

        void ParseOther() {
            Y_ENSURE(*(Line_++) == "//! reflect:other");
            for (; Line_ != std::end(Lines_); ++Line_) {
                if (Line_->empty() || Line_->StartsWith("//")) {
                    continue;
                }

                TString name, content;
                ParseStatement(name, content);

                Meta_.Other.emplace(name);
                Meta_.ContentByName.emplace(std::move(name), std::move(content));
            }
        }

        void ParseStatement(TString& name, TString& content) {
            if (!ParseTrivialLexerRule(TString(*Line_), name, content)) {
                TString multiline = ParseMultilineStatement();
                Y_ENSURE(ParseTrivialLexerRule(std::move(multiline), name, content));
            }
        }

        TString ParseMultilineStatement() {
            TString multiline = std::move(*Line_);
            for (; *Line_ != ";"; ++Line_) {
                if (Line_->empty() || Line_->StartsWith("//")) {
                    continue;
                }

                multiline += std::move(*Line_);
                multiline += ' ';
            }
            multiline += ";";

            Y_ENSURE(*(Line_++) == ";");
            return multiline;
        }

        TVector<TString> Lines_;
        TVector<TString>::iterator Line_;
        TGrammarMeta Meta_;
    };

    TGrammarMeta GetGrammarMeta() {
        TString grammar;
        Y_ENSURE(NResource::FindExact("SQLv1Antlr4.g.in", &grammar));

        return TGrammarMetaParser(std::move(grammar)).ExtractMeta();
    }

} // namespace NSQLReflect
