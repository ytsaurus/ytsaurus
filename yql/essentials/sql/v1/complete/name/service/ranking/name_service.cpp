#include "name_service.h"

#include <yql/essentials/sql/v1/complete/name/service/namespacing.h>

namespace NSQLComplete {

    namespace {

        class TNameService: public INameService {
        public:
            TNameService(INameService::TPtr source, IRanking::TPtr ranking)
                : Source_(std::move(source))
                , Ranking_(std::move(ranking))
            {
            }

            NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) override {
                // TODO(YQL-19747): Waiting without a timeout and error checking
                TNameResponse response = Source_->Lookup(request).ExtractValueSync();

                InsertNamespace(response.RankedNames, request);
                Ranking_->CropToSortedPrefix(response.RankedNames, request.Limit);
                RemoveNamespace(response.RankedNames, request);

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
