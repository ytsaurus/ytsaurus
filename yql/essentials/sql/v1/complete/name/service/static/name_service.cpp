#include "name_service.h"

#include <yql/essentials/sql/v1/complete/name/service/ranking/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/ranking/ranking.h>
#include <yql/essentials/sql/v1/complete/text/case.h>

#include <library/cpp/threading/future/wait/wait.h>

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

    void FixPrefix(TVector<TGenericName>& names, const TNameRequest& request) {
        for (auto& name : names) {
            FixPrefix(name, request);
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
                auto prefix = Prefixed(request.Prefix, ".", *request.Constraints.Pragma);
                auto names = FilteredByPrefix(prefix, Pragmas_);
                AppendAs<TPragmaName>(response.RankedNames, names);
            }

            FixPrefix(response.RankedNames, request);

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
                auto prefix = Prefixed(request.Prefix, "::", *request.Constraints.Function);
                auto names = FilteredByPrefix(prefix, Functions_);
                AppendAs<TFunctionName>(response.RankedNames, names);
            }

            FixPrefix(response.RankedNames, request);

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
        explicit TStaticNameService(NameSet names)
            : NameSet_(std::move(names))
            , Keyword_()
            , PragmaName_(std::move(NameSet_.Pragmas))
            , TypeName_(std::move(NameSet_.Types))
            , FunctionName_(std::move(NameSet_.Functions))
            , HintName_(std::move(NameSet_.Hints))
        {
            Sort(NameSet_.Tables, NoCaseCompare);
        }

        NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) override {
            TNameResponse response;

            TVector<NThreading::TFuture<TNameResponse>> subresponses;
            if (!request.Keywords.empty()) {
                subresponses.emplace_back(Keyword_.Lookup(request));
            }
            if (request.Constraints.Pragma) {
                subresponses.emplace_back(PragmaName_.Lookup(request));
            }
            if (request.Constraints.Type) {
                subresponses.emplace_back(TypeName_.Lookup(request));
            }
            if (request.Constraints.Function) {
                subresponses.emplace_back(FunctionName_.Lookup(request));
            }
            if (request.Constraints.Hint) {
                subresponses.emplace_back(HintName_.Lookup(request));
            }

            // TODO(YQL-19747): Waiting without a timeout and error checking
            NThreading::WaitExceptionOrAll(subresponses).GetValueSync();

            for (auto& subrespons : subresponses) {
                const auto& names = subrespons.GetValueSync().RankedNames;
                std::ranges::copy(names, std::back_inserter(response.RankedNames));
            }

            // TODO(YQL-19747): Extract to schema service
            if (request.Constraints.Table) {
                AppendAs<TTableName>(
                    response.RankedNames,
                    FilteredByPrefix(request.Prefix, NameSet_.Tables));
            }

            return NThreading::MakeFuture(std::move(response));
        }

    private:
        NameSet NameSet_;

        TKeywordNameService Keyword_;
        TPragmaNameService PragmaName_;
        TTypeNameService TypeName_;
        TFunctionNameService FunctionName_;
        THintNameService HintName_;
    };

    INameService::TPtr MakeStaticNameService() {
        return MakeStaticNameService(MakeDefaultNameSet(), MakeDefaultRanking());
    }

    INameService::TPtr MakeStaticNameService(NameSet names, IRanking::TPtr ranking) {
        return MakeRankingNameService(
            MakeHolder<TStaticNameService>(std::move(names)), std::move(ranking));
    }

} // namespace NSQLComplete
