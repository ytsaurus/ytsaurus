#include <yt/yt/client/hedging/cache.h>
#include <yt/yt_proto/yt/client/hedging/proto/config.pb.h>

#include <yt/yt/core/misc/format.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/vector.h>

#include <util/system/env.h>

#include <thread>

namespace NYT::NHedgingClient::NRpc {

// yt does not create physical connection immediately, so try to use this fact to create connection to non existence server
TEST(TClientsCacheTest, GetSameClient) {
    SetEnv("YT_TOKEN", "AAAA-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto cache = CreateClientsCache();
    auto client1 = cache->GetClient("localhost");
    auto client2 = cache->GetClient("localhost");
    EXPECT_TRUE(client1 == client2);
}

TEST(TClientsCacheTest, GetClientWithProxyRole) {
    SetEnv("YT_TOKEN", "AAAA-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto cache = CreateClientsCache();
    auto client1 = cache->GetClient("bigb@localhost");
    auto client2 = cache->GetClient("localhost");
    EXPECT_TRUE(client1 != client2);
}

TEST(TClientsCacheTest, MultiThreads) {
    SetEnv("YT_TOKEN", "AAAA-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    auto cache = CreateClientsCache();
    TVector<std::thread> threads(Reserve(10));
    TVector<NApi::IClientPtr> clients(threads.capacity());
    TVector<size_t> collisions(threads.capacity());

    for (size_t i = 0 ; i < threads.capacity(); ++i) {
        threads.emplace_back([=, &clients, &collisions]() {
            try {
                for (size_t j = 0; j < 1000; ++j) {
                    auto client = cache->GetClient(Format("localhost:6000%v", i));
                    if (client != clients[i]) {
                        clients[i] = client;
                        ++collisions[i];
                    }
                }
            } catch (...) {
                collisions[i] = 100500; // exception marker
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
    for (const auto& client : clients) {
        EXPECT_TRUE(client);
    }
    for (auto collision : collisions) {
        EXPECT_EQ(1u, collision);
    }
}

} // namespace NYT::NHedgingClient::NRpc
