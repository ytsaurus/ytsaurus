#include "schema_gateway.h"

namespace NSQLComplete {

    namespace {

        class TSchemaGateway: public ISchemaGateway {
        public:
            explicit TSchemaGateway(THashMap<TString, ISchemaGateway::TPtr> mapping)
                : Mapping_(std::move(mapping))
            {
            }

            NThreading::TFuture<TListResponse> List(const TListRequest& request) const override {
                auto iter = Mapping_.find(request.Cluster);
                if (iter == std::end(Mapping_)) {
                    yexception e;
                    e << "unknown cluster '" << request.Cluster << "'";
                    std::exception_ptr p = std::make_exception_ptr(e);
                    return NThreading::MakeErrorFuture<TListResponse>(p);
                }

                return iter->second->List(request);
            }

        private:
            THashMap<TString, ISchemaGateway::TPtr> Mapping_;
        };

    } // namespace

    ISchemaGateway::TPtr MakeDispatchSchemaGateway(THashMap<TString, ISchemaGateway::TPtr> mapping) {
        return new TSchemaGateway(std::move(mapping));
    }

} // namespace NSQLComplete
