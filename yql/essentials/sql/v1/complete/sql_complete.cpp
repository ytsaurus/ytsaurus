#include "sql_complete.h"

#include "sql_context.h"
#include "string_util.h"

#include <yql/essentials/sql/v1/complete/name/static/name_service.h>

#include <util/generic/algorithm.h>
#include <util/charset/utf8.h>

namespace NSQLComplete {

    class TSqlCompletionEngine: public ISqlCompletionEngine {
        const struct {
            size_t Limit = 128;
            TDuration Timeout = TDuration::Seconds(2);
        } Configuration; // TODO: Dependency Injection

    public:
        explicit TSqlCompletionEngine(INameService::TPtr names)
            : ContextInference(MakeSqlContextInference())
            , Names(std::move(names))
        {
        }

        TCompletion Complete(TCompletionInput input) {
            auto prefix = input.Text.Head(input.CursorPosition);
            auto completedToken = GetCompletedToken(prefix);

            auto context = ContextInference->Analyze(input);

            TVector<TCandidate> candidates;
            EnrichWithKeywords(candidates, std::move(context.Keywords), completedToken);
            EnrichWithNames(candidates, context, completedToken);

            return {
                .CompletedToken = std::move(completedToken),
                .Candidates = std::move(candidates),
            };
        }

    private:
        TCompletedToken GetCompletedToken(TStringBuf prefix) {
            return {
                .Content = LastWord(prefix),
                .SourcePosition = LastWordIndex(prefix),
            };
        }

        void EnrichWithKeywords(
            TVector<TCandidate>& candidates,
            TVector<TString> keywords,
            const TCompletedToken& prefix) {
            for (auto keyword : keywords) {
                candidates.push_back({
                    .Kind = ECandidateKind::Keyword,
                    .Content = std::move(keyword),
                });
            }
            FilterByContent(candidates, prefix.Content);
            candidates.crop(Configuration.Limit);
        }

        void EnrichWithNames(
            TVector<TCandidate>& candidates,
            const TCompletionContext& context,
            const TCompletedToken& prefix) {
            if (candidates.size() == Configuration.Limit) {
                return;
            }

            TNameRequest request = {
                .Prefix = TString(prefix.Content),
                .Limit = Configuration.Limit - candidates.size(),
            };

            if (context.IsTypeName) {
                request.Constraints.TypeName = TTypeName::TConstraints();
            }

            auto future = Names->Lookup(std::move(request));
            if (!future.Wait(Configuration.Timeout)) {
                return; // TODO: Add Error Listener
            }
            TNameResponse response = future.ExtractValueSync();

            EnrichWithNames(candidates, std::move(response.RankedNames));
        }

        void EnrichWithNames(TVector<TCandidate>& candidates, TVector<TGenericName> names) {
            for (auto& name : names) {
                candidates.emplace_back(std::visit([](auto&& name) -> TCandidate {
                    using T = std::decay_t<decltype(name)>;
                    if constexpr (std::is_base_of_v<TIndentifier, T>) {
                        return {ECandidateKind::TypeName, std::move(name.Indentifier)};
                    }
                }, std::move(name)));
            }
        }

        // TODO: make the yql/essentials/sql/v1/complete/util/string.h
        void FilterByContent(TVector<TCandidate>& candidates, TStringBuf prefix) {
            const auto lowerPrefix = ToLowerUTF8(prefix);
            auto removed = std::ranges::remove_if(candidates, [&](const auto& candidate) {
                return !ToLowerUTF8(candidate.Content).StartsWith(lowerPrefix);
            });
            candidates.erase(std::begin(removed), std::end(removed));
        }

        ISqlContextInference::TPtr ContextInference;
        INameService::TPtr Names;
    };

    ISqlCompletionEngine::TPtr MakeSqlCompletionEngine() {
        return MakeSqlCompletionEngine(MakeStaticNameService(MakeDefaultNameSet()));
    }

    ISqlCompletionEngine::TPtr MakeSqlCompletionEngine(INameService::TPtr names) {
        return ISqlCompletionEngine::TPtr(new TSqlCompletionEngine(std::move(names)));
    }

} // namespace NSQLComplete

template <>
void Out<NSQLComplete::ECandidateKind>(IOutputStream& out, NSQLComplete::ECandidateKind kind) {
    switch (kind) {
        case NSQLComplete::ECandidateKind::Keyword:
            out << "Keyword";
            break;
        case NSQLComplete::ECandidateKind::TypeName:
            out << "TypeName";
            break;
    }
}

template <>
void Out<NSQLComplete::TCandidate>(IOutputStream& out, const NSQLComplete::TCandidate& candidate) {
    out << "(" << candidate.Kind << ": " << candidate.Content << ")";
}
