#include "name_service.h"

#include "name_index.h"

#include <yql/essentials/sql/v1/complete/name/service/ranking/ranking.h>
#include <yql/essentials/sql/v1/complete/name/service/union/name_service.h>
#include <yql/essentials/sql/v1/complete/text/case.h>

namespace NSQLComplete {

    const TVector<TStringBuf> FilteredByPrefix(const TString& prefix, const TNameIndex& index Y_LIFETIME_BOUND) {
        TNameIndexEntry normalized = {
            .Normalized = NormalizeName(prefix),
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

    class TRankingNameService: public INameService {
    private:
        auto Ranking(TNameRequest request) const {
            return [request = std::move(request), this](auto f) {
                TNameResponse response = f.ExtractValue();
                auto& names = response.RankedNames;

                names = request.Constraints.Qualified(std::move(names));
                Ranking_->CropToSortedPrefix(names, request.Limit);
                names = request.Constraints.Unqualified(std::move(names));

                return response;
            };
        }

    public:
        explicit TRankingNameService(IRanking::TPtr ranking)
            : Ranking_(std::move(ranking))
        {
        }

        NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) const override {
            return LookupUnranked(request).Apply(Ranking(request));
        }

        virtual NThreading::TFuture<TNameResponse> LookupUnranked(TNameRequest request) const = 0;

    private:
        IRanking::TPtr Ranking_;
    };

    class TKeywordNameService: public TRankingNameService {
    public:
        explicit TKeywordNameService(IRanking::TPtr ranking)
            : TRankingNameService(std::move(ranking))
        {
        }

        NThreading::TFuture<TNameResponse> LookupUnranked(TNameRequest request) const override {
            TNameResponse response;
            Sort(request.Keywords, NoCaseCompare);
            AppendAs<TKeyword>(
                response.RankedNames,
                FilteredByPrefix(request.Prefix, request.Keywords));
            return NThreading::MakeFuture<TNameResponse>(std::move(response));
        }
    };

    class TPragmaNameService: public TRankingNameService {
    public:
        explicit TPragmaNameService(IRanking::TPtr ranking, TVector<TString> pragmas)
            : TRankingNameService(std::move(ranking))
            , Pragmas_(BuildNameIndex(std::move(pragmas), NormalizeName))
        {
        }

        NThreading::TFuture<TNameResponse> LookupUnranked(TNameRequest request) const override {
            TNameResponse response;
            if (request.Constraints.Pragma) {
                TPragmaName prefix;
                prefix.Indentifier = request.Prefix;
                prefix = request.Constraints.Qualified(std::move(prefix));
                AppendAs<TPragmaName>(
                    response.RankedNames,
                    FilteredByPrefix(prefix.Indentifier, Pragmas_));
            }
            response.RankedNames = request.Constraints.Unqualified(std::move(response.RankedNames));
            return NThreading::MakeFuture<TNameResponse>(std::move(response));
        }

    private:
        TNameIndex Pragmas_;
    };

    class TTypesNameService: public TRankingNameService {
    public:
        explicit TTypesNameService(IRanking::TPtr ranking, TVector<TString> types)
            : TRankingNameService(std::move(ranking))
            , Types_(BuildNameIndex(std::move(types), NormalizeName))
        {
        }

        NThreading::TFuture<TNameResponse> LookupUnranked(TNameRequest request) const override {
            TNameResponse response;
            if (request.Constraints.Type) {
                AppendAs<TTypeName>(
                    response.RankedNames,
                    FilteredByPrefix(request.Prefix, Types_));
            }
            return NThreading::MakeFuture<TNameResponse>(std::move(response));
        }

    private:
        TNameIndex Types_;
    };

    class TFunctionsNameService: public TRankingNameService {
    public:
        explicit TFunctionsNameService(IRanking::TPtr ranking, TVector<TString> functions)
            : TRankingNameService(std::move(ranking))
            , Functions_(BuildNameIndex(std::move(functions), NormalizeName))
        {
        }

        NThreading::TFuture<TNameResponse> LookupUnranked(TNameRequest request) const override {
            TNameResponse response;
            if (request.Constraints.Function) {
                TFunctionName prefix;
                prefix.Indentifier = request.Prefix;
                prefix = request.Constraints.Qualified(std::move(prefix));
                AppendAs<TFunctionName>(
                    response.RankedNames,
                    FilteredByPrefix(prefix.Indentifier, Functions_));
            }
            response.RankedNames = request.Constraints.Unqualified(std::move(response.RankedNames));
            return NThreading::MakeFuture<TNameResponse>(std::move(response));
        }

    private:
        TNameIndex Functions_;
    };

    class THintsNameService: public TRankingNameService {
    public:
        explicit THintsNameService(
            IRanking::TPtr ranking,
            THashMap<EStatementKind, TVector<TString>> hints)
            : TRankingNameService(std::move(ranking))
            , Hints_([hints = std::move(hints)] {
                THashMap<EStatementKind, TNameIndex> index;
                for (auto& [k, hints] : hints) {
                    index.emplace(k, BuildNameIndex(std::move(hints), NormalizeName));
                }
                return index;
            }())
        {
        }

        NThreading::TFuture<TNameResponse> LookupUnranked(TNameRequest request) const override {
            TNameResponse response;
            if (request.Constraints.Hint) {
                const auto stmt = request.Constraints.Hint->Statement;
                if (const auto* hints = Hints_.FindPtr(stmt)) {
                    AppendAs<THintName>(
                        response.RankedNames,
                        FilteredByPrefix(request.Prefix, *hints));
                }
            }
            return NThreading::MakeFuture<TNameResponse>(std::move(response));
        }

    private:
        THashMap<EStatementKind, TNameIndex> Hints_;
    };

    INameService::TPtr MakeStaticNameService(TNameSet names, TFrequencyData frequency) {
        return MakeStaticNameService(
            Pruned(std::move(names), frequency),
            MakeDefaultRanking(std::move(frequency)));
    }

    INameService::TPtr MakeStaticNameService(TNameSet names, IRanking::TPtr ranking) {
        return MakeUnionNameService({
                                        new TKeywordNameService(ranking),
                                        new TPragmaNameService(ranking, std::move(names.Pragmas)),
                                        new TTypesNameService(ranking, std::move(names.Types)),
                                        new TFunctionsNameService(ranking, std::move(names.Functions)),
                                        new THintsNameService(ranking, std::move(names.Hints)),
                                    }, ranking);
    }

} // namespace NSQLComplete
