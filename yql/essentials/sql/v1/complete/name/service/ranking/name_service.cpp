#include "name_service.h"

namespace NSQLComplete {

    namespace {

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

        void InstallPrefix(TGenericName& name, const TNameRequest& request) {
            std::visit([&](auto& name) -> size_t {
                using T = std::decay_t<decltype(name)>;
                if constexpr (std::is_same_v<T, TPragmaName>) {
                    name.Indentifier = Prefixed(name.Indentifier, ".", *request.Constraints.Pragma);
                }
                if constexpr (std::is_same_v<T, TFunctionName>) {
                    name.Indentifier = Prefixed(name.Indentifier, "::", *request.Constraints.Function);
                }
                return 0;
            }, name);
        }

        void InstallPrefix(TVector<TGenericName>& names, const TNameRequest& request) {
            for (auto& name : names) {
                InstallPrefix(name, request);
            }
        }

        class TNameService: public INameService {
        public:
            TNameService(INameService::TPtr source, IRanking::TPtr ranking)
                : Source_(std::move(source))
                , Ranking_(std::move(ranking))
            {
            }

            TFuture<TNameResponse> Lookup(TNameRequest request) override {
                // TODO(YQL-19747): Waiting without a timeout and error checking
                TNameResponse response = Source_->Lookup(request).ExtractValueSync();

                InstallPrefix(response.RankedNames, request);
                Ranking_->CropToSortedPrefix(response.RankedNames, request.Limit);
                FixPrefix(response.RankedNames, request);

                return NThreading::MakeFuture(std::move(response));
            }

        private:
            INameService::TPtr Source_;
            IRanking::TPtr Ranking_;
        };

    } // namespace

    INameService::TPtr MakeRankingNameService(INameService::TPtr source, IRanking::TPtr ranking) {
        return INameService::TPtr(new TNameService(std::move(source), std::move(ranking)));
    }

} // namespace NSQLComplete
