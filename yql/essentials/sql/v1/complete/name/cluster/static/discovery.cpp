#include "discovery.h"

namespace NSQLComplete {

    namespace {

        class TClusterDiscovery: public IClusterDiscovery {
        public:
            explicit TClusterDiscovery(TVector<TString> instances)
                : Instances_(std::move(instances))
            {
            }

            NThreading::TFuture<TClusterCatalog> Query() const override {
                return NThreading::MakeFuture(TClusterCatalog{
                    .Instances = Instances_,
                });
            }

        private:
            TVector<TString> Instances_;
        };

    } // namespace

    IClusterDiscovery::TPtr MakeStaticClusterDiscovery(TVector<TString> instances) {
        return new TClusterDiscovery(std::move(instances));
    }

} // namespace NSQLComplete
