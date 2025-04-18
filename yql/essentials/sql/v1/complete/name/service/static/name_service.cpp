#include "name_service.h"

#include <yql/essentials/sql/v1/complete/name/parse.h>
#include <yql/essentials/sql/v1/complete/name/service/ranking/ranking.h>
#include <yql/essentials/sql/v1/complete/name/service/union/name_service.cpp>
#include <yql/essentials/sql/v1/complete/text/case.h>

#include <library/cpp/threading/future/wait/wait.h>

#include <util/string/cast.h>

namespace NSQLComplete {

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
        for (const S& element : source) {
            if constexpr (std::is_same_v<T, TPragmaName>) {
                target.emplace_back(ParsePragma(element));
            } else if constexpr (std::is_same_v<T, TFunctionName>) {
                target.emplace_back(ParseFunction(element));
            } else {
                target.emplace_back(T{TString(element)});
            }
        }
    }

    class TKeywordNameService: public INameService {
    public:
        NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) override {
            TNameResponse response;

            Sort(request.Keywords, NoCaseCompare);
            AppendAs<TKeyword>(
                response.RankedNames,
                FilteredByPrefix(request.Prefix, request.Keywords));

            return NThreading::MakeFuture(std::move(response));
        }
    };

    class TPragmaNameService: public INameService {
    public:
        explicit TPragmaNameService(TVector<TString> pragmas)
            : Pragmas_(std::move(pragmas))
        {
            Sort(Pragmas_, NoCaseCompare);
        }

        NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) override {
            TNameResponse response;

            if (request.Constraints.Pragma) {
                TPragmaName pragma;
                pragma.Namespace = request.Constraints.Pragma->Namespace;
                pragma.Indentifier = request.Prefix;

                AppendAs<TPragmaName>(
                    response.RankedNames,
                    FilteredByPrefix(ToString(pragma), Pragmas_));
            }

            return NThreading::MakeFuture(std::move(response));
        }

    private:
        TVector<TString> Pragmas_;
    };

    class TTypeNameService: public INameService {
    public:
        explicit TTypeNameService(TVector<TString> types)
            : Types_(std::move(types))
        {
            Sort(Types_, NoCaseCompare);
        }

        NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) override {
            TNameResponse response;

            if (request.Constraints.Type) {
                AppendAs<TTypeName>(
                    response.RankedNames,
                    FilteredByPrefix(request.Prefix, Types_));
            }

            return NThreading::MakeFuture(std::move(response));
        }

    private:
        TVector<TString> Types_;
    };

    class TFunctionNameService: public INameService {
    public:
        explicit TFunctionNameService(TVector<TString> functions)
            : Functions_(std::move(functions))
        {
            Sort(Functions_, NoCaseCompare);
        }

        NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) override {
            TNameResponse response;

            if (request.Constraints.Function) {
                TFunctionName function;
                function.Namespace = request.Constraints.Function->Namespace;
                function.Indentifier = request.Prefix;

                AppendAs<TFunctionName>(
                    response.RankedNames,
                    FilteredByPrefix(ToString(function), Functions_));
            }

            return NThreading::MakeFuture(std::move(response));
        }

    private:
        TVector<TString> Functions_;
    };

    class THintNameService: public INameService {
    public:
        explicit THintNameService(THashMap<EStatementKind, TVector<TString>> Hints)
            : Hints_(std::move(Hints))
        {
            for (auto& [_, hints] : Hints_) {
                Sort(hints, NoCaseCompare);
            }
        }

        NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) override {
            TNameResponse response;

            if (request.Constraints.Hint) {
                const auto stmt = request.Constraints.Hint->Statement;
                AppendAs<THintName>(
                    response.RankedNames,
                    FilteredByPrefix(request.Prefix, Hints_[stmt]));
            }

            return NThreading::MakeFuture(std::move(response));
        }

    private:
        THashMap<EStatementKind, TVector<TString>> Hints_;
    };

    class TStaticNameService: public INameService {
    public:
        explicit TStaticNameService(NameSet names, IRanking::TPtr ranking)
            : NameSet_(std::move(names))
            , Basic_(MakeUnionNameService([&] {
                TVector<INameService::TPtr> children;
                children.emplace_back(new TKeywordNameService());
                children.emplace_back(new TPragmaNameService(std::move(NameSet_.Pragmas)));
                children.emplace_back(new TTypeNameService(std::move(NameSet_.Types)));
                children.emplace_back(new TFunctionNameService(std::move(NameSet_.Functions)));
                children.emplace_back(new THintNameService(std::move(NameSet_.Hints)));
                return children;
            }()))
            , Ranking_(std::move(ranking))
        {
            Sort(NameSet_.Tables, NoCaseCompare);
        }

        NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) override {
            // TODO(YQL-19747): Waiting without a timeout and error checking
            TNameResponse response = Basic_->Lookup(request).GetValueSync();

            // TODO(YQL-19747): Extract to schema service
            if (request.Constraints.Table) {
                AppendAs<TTableName>(
                    response.RankedNames,
                    FilteredByPrefix(request.Prefix, NameSet_.Tables));
            }

            Ranking_->CropToSortedPrefix(response.RankedNames, request.Limit);

            return NThreading::MakeFuture(std::move(response));
        }

    private:
        NameSet NameSet_;
        INameService::TPtr Basic_;
        IRanking::TPtr Ranking_;
    };

    INameService::TPtr MakeStaticNameService() {
        return MakeStaticNameService(MakeDefaultNameSet(), MakeDefaultRanking());
    }

    INameService::TPtr MakeStaticNameService(NameSet names, IRanking::TPtr ranking) {
        return INameService::TPtr(new TStaticNameService(std::move(names), std::move(ranking)));
    }

} // namespace NSQLComplete
