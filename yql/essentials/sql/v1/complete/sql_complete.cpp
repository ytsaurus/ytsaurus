#include "sql_complete.h"

#include "sql_context.h"
#include "string_util.h"

#include <util/generic/algorithm.h>
#include <util/charset/utf8.h>

namespace NSQLComplete {

    class TSqlCompletionEngine: public ISqlCompletionEngine {
    public:
        TSqlCompletionEngine()
            : ContextInference(MakeSqlContextInference())
        {
        }

        TCompletion Complete(TCompletionInput input) {
            auto prefix = input.Text.Head(input.CursorPosition);
            auto completedToken = GetCompletedToken(prefix);

            auto context = ContextInference->Analyze(input);

            auto candidates = GenerateCandidates(std::move(context));

            FilterByContent(candidates, completedToken.Content);

            RankingSort(candidates);

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

        TVector<TCandidate> GenerateCandidates(TCompletionContext context) {
            TVector<TCandidate> candidates;
            EnrichWithKeywords(candidates, context.Keywords);
            if (context.IsTypeName) {
                EnrichWithTypeNames(candidates);
            }
            return candidates;
        }

        void EnrichWithKeywords(TVector<TCandidate>& candidates, TVector<TString> keywords) {
            for (auto keyword : keywords) {
                candidates.push_back({
                    .Kind = ECandidateKind::Keyword,
                    .Content = std::move(keyword),
                });
            }
        }

        void EnrichWithTypeNames(TVector<TCandidate>& candidates) {
            candidates.push_back({
                .Kind = ECandidateKind::TypeName,
                .Content = "Uint64",
            });
        }

        void FilterByContent(TVector<TCandidate>& candidates, TStringBuf prefix) {
            const auto lowerPrefix = ToLowerUTF8(prefix);
            auto removed = std::ranges::remove_if(candidates, [&](const auto& candidate) {
                return !ToLowerUTF8(candidate.Content).StartsWith(lowerPrefix);
            });
            candidates.erase(std::begin(removed), std::end(removed));
        }

        void RankingSort(TVector<TCandidate>& candidates) {
            Sort(candidates, [](const TCandidate& lhs, const TCandidate& rhs) {
                return std::tie(lhs.Kind, lhs.Content) < std::tie(rhs.Kind, rhs.Content);
            });
        }

        ISqlContextInference::TPtr ContextInference;
    };

    ISqlCompletionEngine::TPtr MakeSqlCompletionEngine() {
        return ISqlCompletionEngine::TPtr(new TSqlCompletionEngine());
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
