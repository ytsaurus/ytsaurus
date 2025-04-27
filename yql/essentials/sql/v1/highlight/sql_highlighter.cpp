#include "sql_highlighter.h"

#include <contrib/libs/re2/re2/re2.h>

#include <util/generic/deque.h>
#include <util/generic/maybe.h>

namespace NSQLHighlight {

    namespace {

        struct TCompiledPattern {
            THolder<RE2> Body;
            THolder<RE2> After;
        };

        struct TCompiledUnit {
            EUnitKind Kind;
            TVector<TCompiledPattern> Patterns;
        };

        struct TCompiledHighlighting {
            TVector<TCompiledUnit> Units;
            TCompiledPattern Whitespace;
        };

        TCompiledPattern Compile(TPattern pattern) {
            RE2::Options options;
            options.set_case_sensitive(!pattern.IsCaseInsensitive);
            options.set_longest_match(pattern.IsLongestMatch);

            return {
                .Body = MakeHolder<RE2>(pattern.BodyRe, options),
                .After = MakeHolder<RE2>(pattern.AfterRe),
            };
        }

        TCompiledUnit Compile(TUnit unit) {
            TCompiledUnit compiled;
            compiled.Kind = unit.Kind;
            for (auto& pattern : unit.Patterns) {
                compiled.Patterns.emplace_back(Compile(std::move(pattern)));
            }
            return compiled;
        }

        TVector<TCompiledUnit> Compile(TVector<TUnit> units) {
            TVector<TCompiledUnit> compiled;
            for (auto& unit : units) {
                compiled.emplace_back(std::move(Compile(unit)));
            }
            return compiled;
        }

        TCompiledHighlighting Compile(THighlighting highlighting) {
            return {
                .Units = Compile(highlighting.Units),
                .Whitespace = Compile(highlighting.Whitespace),
            };
        }

        class THighlighter: public IHighlighter {
        public:
            explicit THighlighter(THighlighting highlighting)
                : Highlighting_(Compile(std::move(highlighting)))
            {
            }

            void Tokenize(TStringBuf text, const TTokenCallback& onNext) const override {
                size_t pos = 0;
                while (pos < text.size()) {
                    auto matched = Match(TStringBuf(text, pos));
                    matched.Begin = pos;
                    pos += matched.Length;
                    onNext(std::move(matched));
                }
            }

        private:
            TToken Match(const TStringBuf prefix) const {
                static constexpr TToken Unknown = {
                    .Kind = EUnitKind::Whitespace,
                    .Begin = 0,
                    .Length = 1,
                };

                TVector<TToken> matched;
                Match(prefix, [&](TToken&& token) {
                    matched.emplace_back(std::move(token));
                });

                auto max = MaxElementBy(
                    matched, [](const TToken& m) { return m.Length; });

                if (max != std::end(matched)) {
                    return *max;
                }
                return Unknown;
            }

            void Match(const TStringBuf prefix, auto onMatch) const {
                for (const auto& unit : Highlighting_.Units) {
                    Match(prefix, unit, onMatch);
                }
            }

            void Match(const TStringBuf prefix, const TCompiledUnit& unit, auto onMatch) const {
                for (const auto& pattern : unit.Patterns) {
                    if (auto content = Match(prefix, pattern)) {
                        onMatch(TToken{
                            .Kind = unit.Kind,
                            .Begin = 0,
                            .Length = content->size(),
                        });
                    }
                }
            }

            TMaybe<TStringBuf> Match(const TStringBuf prefix, const TCompiledPattern& pattern) const {
                TMaybe<TStringBuf> body, after;
                if ((body = Match(prefix, *pattern.Body)) &&
                    (after = Match(prefix.Tail(body->size()), *pattern.After))) {
                    return body;
                }
                return Nothing();
            }

            TMaybe<TStringBuf> Match(const TStringBuf prefix, const RE2& regex) const {
                re2::StringPiece input(prefix.data(), prefix.size());
                if (RE2::Consume(&input, regex)) {
                    return TStringBuf(prefix.data(), input.data());
                }
                return Nothing();
            }

            TCompiledHighlighting Highlighting_;
        };

    } // namespace

    TVector<TToken> Tokenize(IHighlighter& highlighter, TStringBuf text) {
        TVector<TToken> tokens;
        highlighter.Tokenize(text, [&](TToken&& token) {
            tokens.emplace_back(std::move(token));
        });
        return tokens;
    }

    IHighlighter::TPtr MakeHighlighter(THighlighting highlighting) {
        return IHighlighter::TPtr(new THighlighter(std::move(highlighting)));
    }

} // namespace NSQLHighlight
