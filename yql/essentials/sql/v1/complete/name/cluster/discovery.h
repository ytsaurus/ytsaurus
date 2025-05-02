#pragma once

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/ptr.h>

namespace NSQLComplete {

    struct TClusterCatalog {
        TVector<TString> Instances;
    };

    class IClusterDiscovery: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<IClusterDiscovery>;

        virtual ~IClusterDiscovery() = default;
        virtual NThreading::TFuture<TClusterCatalog> Query() const = 0;
    };

} // namespace NSQLComplete
