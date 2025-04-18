#include "name_service.h"

#include <library/cpp/threading/future/wait/wait.h>

namespace NSQLComplete {

    namespace {

        class TNameService: public INameService {
        public:
            explicit TNameService(TVector<INameService::TPtr> children)
                : Children_(std::move(children))
            {
            }

            NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) override {
                TVector<NThreading::TFuture<TNameResponse>> responses;
                for (auto& child : Children_) {
                    responses.emplace_back(child->Lookup(request));
                }
                // TODO(YQL-19747): Waiting without a timeout and error checking
                NThreading::WaitExceptionOrAll(responses).GetValueSync();

                TNameResponse response;
                for (auto& part : responses) {
                    const auto& names = part.GetValueSync().RankedNames;
                    std::ranges::copy(names, std::back_inserter(response.RankedNames));
                }

                return NThreading::MakeFuture(response);
            }

        private:
            TVector<INameService::TPtr> Children_;
        };

    } // namespace

    INameService::TPtr MakeUnionNameService(TVector<INameService::TPtr> children) {
        return INameService::TPtr(new TNameService(std::move(children)));
    }

} // namespace NSQLComplete
