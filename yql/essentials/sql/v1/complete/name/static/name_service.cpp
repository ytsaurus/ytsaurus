#include "name_service.h"

#include "ranking.h"

#include <yql/essentials/sql/v1/complete/text/case.h>

#include <yql/essentials/core/sql_types/normalize_name.h>

namespace NSQLComplete {

    struct TNameIndexEntry {
        TString Normalized;
        TString Original;
    };

    using TNameIndex = TVector<TNameIndexEntry>;

    bool NameIndexCompare(const TNameIndexEntry& lhs, const TNameIndexEntry& rhs) {
        return NoCaseCompare(lhs.Normalized, rhs.Normalized);
    }

    auto NameIndexCompareLimit(size_t limit) {
        return [cmp = NoCaseCompareLimit(limit)](const TNameIndexEntry& lhs, const TNameIndexEntry& rhs) {
            return cmp(lhs.Normalized, rhs.Normalized);
        };
    }

    TNameIndex BuildNameIndex(TVector<TString> originals) {
        TNameIndex index;
        for (auto& original : originals) {
            TNameIndexEntry entry = {
                .Normalized = NYql::NormalizeName(original),
                .Original = std::move(original),
            };
            index.emplace_back(std::move(entry));
        }

        Sort(index, NameIndexCompare);
        return index;
    }

    const TVector<TStringBuf> FilteredByPrefix(const TString& prefix, const TNameIndex& index Y_LIFETIME_BOUND) {
        TNameIndexEntry normalized = {
            .Normalized = NYql::NormalizeName(prefix),
            .Original = "",
        };

        auto range = std::ranges::equal_range(
            std::begin(index), std::end(index),
            normalized, NameIndexCompareLimit(normalized.Normalized.size()));

        TVector<TStringBuf> filtered;
        for (const TNameIndexEntry& entry : range) {
            filtered.emplace_back(TStringBuf(entry.Original));
        }
        return filtered;
    }

    const TVector<TStringBuf> FilteredByPrefix(
        const TString& prefix,
        const TVector<TString>& sorted Y_LIFETIME_BOUND) {
        auto [first, last] = EqualRange(
            std::begin(sorted), std::end(sorted),
            prefix, NoCaseCompareLimit(prefix.size()));
        return TVector<TStringBuf>(first, last);
    }

    template <class T, class S = TStringBuf>
    void AppendAs(TVector<TGenericName>& target, const TVector<S>& source) {
        for (const auto& element : source) {
            target.emplace_back(T{TString(element)});
        }
    }

    TString Prefixed(const TStringBuf requestPrefix, const TStringBuf delimeter, const TNamespaced& namespaced) {
        TString prefix;
        if (!namespaced.Namespace.empty()) {
            prefix += namespaced.Namespace;
            prefix += delimeter;
        }
        prefix += requestPrefix;
        return prefix;
    }

    void FixPrefix(TString& name, const TStringBuf delimeter, const TNamespaced& namespaced) {
        if (namespaced.Namespace.empty()) {
            return;
        }
        name.remove(0, namespaced.Namespace.size() + delimeter.size());
    }

    void FixPrefix(TGenericName& name, const TNameRequest& request) {
        std::visit([&](auto& name) -> size_t {
            using T = std::decay_t<decltype(name)>;
            if constexpr (std::is_same_v<T, TPragmaName>) {
                FixPrefix(name.Indentifier, ".", *request.Constraints.Pragma);
            }
            if constexpr (std::is_same_v<T, TFunctionName>) {
                FixPrefix(name.Indentifier, "::", *request.Constraints.Function);
            }
            return 0;
        }, name);
    }

    class TStaticNameService: public INameService {
    public:
        explicit TStaticNameService(NameSet names, IRanking::TPtr ranking)
            : Pragmas_(BuildNameIndex(std::move(names.Pragmas)))
            , Types_(BuildNameIndex(std::move(names.Types)))
            , Functions_(BuildNameIndex(std::move(names.Functions)))
            , Hints_([hints = std::move(names.Hints)] {
                THashMap<EStatementKind, TNameIndex> index;
                for (auto& [k, hints] : hints) {
                    index.emplace(k, BuildNameIndex(std::move(hints)));
                }
                return index;
            }())
            , Ranking_(std::move(ranking))
        {
        }

        TFuture<TNameResponse> Lookup(TNameRequest request) override {
            TNameResponse response;

            Sort(request.Keywords, NoCaseCompare);
            AppendAs<TKeyword>(
                response.RankedNames,
                FilteredByPrefix(request.Prefix, request.Keywords));

            if (request.Constraints.Pragma) {
                auto prefix = Prefixed(request.Prefix, ".", *request.Constraints.Pragma);
                auto names = FilteredByPrefix(prefix, Pragmas_);
                AppendAs<TPragmaName>(response.RankedNames, names);
            }

            if (request.Constraints.Type) {
                AppendAs<TTypeName>(
                    response.RankedNames,
                    FilteredByPrefix(request.Prefix, Types_));
            }

            if (request.Constraints.Function) {
                auto prefix = Prefixed(request.Prefix, "::", *request.Constraints.Function);
                auto names = FilteredByPrefix(prefix, Functions_);
                AppendAs<TFunctionName>(response.RankedNames, names);
            }

            if (request.Constraints.Hint) {
                const auto stmt = request.Constraints.Hint->Statement;
                AppendAs<THintName>(
                    response.RankedNames,
                    FilteredByPrefix(request.Prefix, Hints_[stmt]));
            }

            Ranking_->CropToSortedPrefix(response.RankedNames, request.Limit);

            for (auto& name : response.RankedNames) {
                FixPrefix(name, request);
            }

            return NThreading::MakeFuture(std::move(response));
        }

    private:
        TNameIndex Pragmas_;
        TNameIndex Types_;
        TNameIndex Functions_;
        THashMap<EStatementKind, TNameIndex> Hints_;
        IRanking::TPtr Ranking_;
    };

    INameService::TPtr MakeStaticNameService() {
        return MakeStaticNameService(MakeDefaultNameSet(), MakeDefaultRanking());
    }

    INameService::TPtr MakeStaticNameService(NameSet names, IRanking::TPtr ranking) {
        return INameService::TPtr(new TStaticNameService(std::move(names), std::move(ranking)));
    }

} // namespace NSQLComplete
