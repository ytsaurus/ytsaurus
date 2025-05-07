#include "name_service.h"

#include <yql/essentials/utils/log/log.h>

namespace NSQLComplete {

    namespace {

        using NYql::NLog::EComponent::SqlComplete;
        using NYql::NLog::ELevel::WARN;

        class TNameService: public INameService {
        public:
            explicit TNameService(INameService::TPtr origin)
                : Origin_(std::move(origin))
            {
            }

            NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) const override {
                return Origin_->Lookup(request).Apply([](auto f) {
                    try {
                        return f.ExtractValue();
                    } catch (const std::exception& e) {
                        YQL_CVLOG(WARN, SqlComplete) << "Lookup failed: " << e.what();
                        throw;
                    }
                });
            }

        private:
            INameService::TPtr Origin_;
        };

    } // namespace

    INameService::TPtr MakeLoggingNameService(INameService::TPtr origin) {
        return new TNameService(std::move(origin));
    }

} // namespace NSQLComplete
