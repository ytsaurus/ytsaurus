#include "name_service.h"

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/utils/log/log.h>

namespace NSQLComplete {

    namespace {

        using NYql::NLog::EComponent::SqlCompletion;
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
                    } catch (...) {
                        YQL_CVLOG(WARN, SqlCompletion) << "Lookup failed: " << CurrentExceptionMessage();
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
