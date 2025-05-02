#include "name_service.h"

#include <util/charset/utf8.h>

namespace NSQLComplete {

    namespace {

        class TNameService: public INameService {
        private:
            static auto FilterByName(TString name) {
                return [name = std::move(name)](auto f) {
                    TClusterCatalog catalog = f.ExtractValue();
                    EraseIf(catalog.Instances, [prefix = ToLowerUTF8(name)](const TString& instance) {
                        return !instance.StartsWith(prefix);
                    });
                    return catalog;
                };
            }

            static auto Crop(size_t limit) {
                return [limit](auto f) {
                    TClusterCatalog catalog = f.ExtractValue();
                    catalog.Instances.crop(limit);
                    return catalog;
                };
            }

            static auto ToResponse() {
                return [](auto f) {
                    TClusterCatalog catalog = f.ExtractValue();

                    TNameResponse response;
                    response.RankedNames.reserve(catalog.Instances.size());

                    for (auto& instance : catalog.Instances) {
                        TClusterName name;
                        name.Indentifier = std::move(instance);
                        response.RankedNames.emplace_back(std::move(name));
                    }

                    return response;
                };
            }

        public:
            explicit TNameService(IClusterDiscovery::TPtr discovery)
                : Discovery_(std::move(discovery))
            {
            }

            NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) const override {
                if (!request.Constraints.Cluster) {
                    return NThreading::MakeFuture<TNameResponse>({});
                }

                return Discovery_->Query()
                    .Apply(FilterByName(request.Prefix))
                    .Apply(Crop(request.Limit))
                    .Apply(ToResponse());
            }

        private:
            IClusterDiscovery::TPtr Discovery_;
        };

    } // namespace

    INameService::TPtr MakeClusterNameService(IClusterDiscovery::TPtr discovery) {
        return new TNameService(std::move(discovery));
    }

} // namespace NSQLComplete
