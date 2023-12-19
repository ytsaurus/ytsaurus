#include <yt/yt/server/lib/hydra/decorated_automaton.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NHydra {
namespace {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger TestLogger("Test");

////////////////////////////////////////////////////////////////////////////////

TEST(TLocalHostNameSanitizer, SingleDataCenterSanitizing)
{
    THashSet<TString> clusterPeers = {
        "m001-cluster-vla.vla.yp-c.yandex.net",
        "m002-cluster-vla.vla.yp-c.yandex.net",
        "m010-cluster-vla.vla.yp-c.yandex.net",
        "m042-cluster-vla.vla.yp-c.yandex.net",
        "m038-cluster-vla.vla.yp-c.yandex.net",
        "m723-cluster-vla.vla.yp-c.yandex.net",
        "m991-cluster-vla.vla.yp-c.yandex.net",
    };

    THashSet<TString> peers = {
        "mc091-cluster-vla.vla.yp-c.yandex.net",
        "clock01-cluster.vla.yp-c.yandex.net",
        "sas7-4539-proxy-cluster.sas.yp-c.yandex.net",
        "sas7-4614-spare-01e-rpc-cluster.sas.yp-c.yandex.net",
        "abc4-2666-bigb-179-tab-sen-v.abc.yp-c.yandex.net",
        "sas7-3822-click-002-tab-cluster.sas.yp-c.yandex.net",
    };

    for (const auto& peerAddress : clusterPeers) {
        EXPECT_EQ("m***-cluster-vla.vla.yp-c.yandex.net", SanitizeLocalHostName(TestLogger, clusterPeers, peerAddress));
    }

    for (const auto& peerAddress : peers) {
        EXPECT_EQ(peerAddress, SanitizeLocalHostName(TestLogger, clusterPeers, peerAddress));
    }
}

TEST(TLocalHostNameSanitizer, CrossDataCenterSanitizing)
{
    THashSet<TString> clusterPeers = {
        "m001-cluster.vla.yp-c.yandex.net",
        "m002-cluster.vlx.yp-c.yandex.net",
        "m010-cluster.man.yp-c.yandex.net",
        "m042-cluster.sas.yp-c.yandex.net",
        "m101-cluster.iva.yp-c.yandex.net",
        "m552-cluster.klg.yp-c.yandex.net",
        "m029-cluster.col.yp-c.yandex.net",
        "m723-cluster.myt.yp-c.yandex.net",
    };

    for (const auto& peerAddress : clusterPeers) {
        EXPECT_EQ("m***-cluster.***.yp-c.yandex.net", SanitizeLocalHostName(TestLogger, clusterPeers, peerAddress));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NHydra
