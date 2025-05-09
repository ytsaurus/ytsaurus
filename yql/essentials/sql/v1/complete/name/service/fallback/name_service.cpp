#include "name_service.h"

#include <library/cpp/threading/future/wait/wait.h>

namespace NSQLComplete {

    namespace {

        class TNameService: public INameService {
        public:
            explicit TNameService(
                INameService::TPtr primary,
                INameService::TPtr standby,
                TFallbackPolicy policy)
                : Primary_(std::move(primary))
                , Standby_(std::move(standby))
                , Policy_(std::move(policy))
            {
            }

            NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) const override {
                auto promise = NThreading::NewPromise<TNameResponse>();
                Primary_->Lookup(request).Apply([promise, request,
                                                 standby = Standby_,
                                                 policy = Policy_](auto f) mutable {
                    try {
                        promise.SetValue(f.ExtractValue());
                    } catch (const std::exception& e) {
                        if (!policy(e)) {
                            promise.SetException(std::current_exception());
                        }

                        standby->Lookup(request).Apply([promise](auto f) mutable {
                            try {
                                promise.SetValue(f.ExtractValue());
                            } catch (...) {
                                promise.SetException(std::current_exception());
                            }
                        });
                    }
                });
                return promise;
            }

        private:
            INameService::TPtr Primary_;
            INameService::TPtr Standby_;
            TFallbackPolicy Policy_;
        };

        class TEmptyNameService: public INameService {
        public:
            NThreading::TFuture<TNameResponse> Lookup(TNameRequest /* request */) const override {
                return NThreading::MakeFuture<TNameResponse>({});
            }
        };

    } // namespace

    INameService::TPtr MakeFallbackNameService(
        INameService::TPtr primary,
        INameService::TPtr standby,
        TFallbackPolicy policy) {
        return new TNameService(std::move(primary), std::move(standby), policy);
    }

    INameService::TPtr MakeEmptyNameService() {
        return new TEmptyNameService();
    }

    INameService::TPtr MakeSwallowingNameService(INameService::TPtr origin, TFallbackPolicy policy) {
        return MakeFallbackNameService(std::move(origin), MakeEmptyNameService(), policy);
    }

} // namespace NSQLComplete
