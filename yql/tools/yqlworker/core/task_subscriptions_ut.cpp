#include "task_subscriptions.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>


using namespace NYql;

Y_UNIT_TEST_SUITE(TTaskSubscriptionsTest) {

    sockaddr_in IpV4Loopback(in_port_t port) {
        sockaddr_in in;
        memset(&in, 0, sizeof(in));
        in.sin_family = AF_INET;
        in.sin_addr = { INADDR_LOOPBACK };
        in.sin_port = port;
        return in;
    }

    sockaddr_in6 IpV6Loopback(in_port_t port) {
        sockaddr_in6 in6;
        memset(&in6, 0, sizeof(in6));
        in6.sin6_family = AF_INET6;
        in6.sin6_addr = IN6ADDR_LOOPBACK_INIT;
        in6.sin6_port = port;
        return in6;
    }

    Y_UNIT_TEST(Subscribe) {
        NAddr::TIPv4Addr lo4(IpV4Loopback(12345));
        NAddr::TIPv6Addr lo6(IpV6Loopback(12345));

        TChannelId ch1 = 0x76543210;
        TChannelId ch2 = ch1 + 1;

        TTaskSubscriptions subscrsMap;
        UNIT_ASSERT_EQUAL(subscrsMap.Size(), 0);

        subscrsMap.Subscribe(lo4, ch1, "jamel", Max<TSubscrMask>());
        UNIT_ASSERT_EQUAL(subscrsMap.Size(), 1);

        // different channelId
        subscrsMap.Subscribe(lo4, ch2, "jamel", Max<TSubscrMask>());
        UNIT_ASSERT_EQUAL(subscrsMap.Size(), 2);

        // different port
        NAddr::TIPv4Addr lo4_1(IpV4Loopback(23456));
        subscrsMap.Subscribe(lo4_1, ch1, "jamel", Max<TSubscrMask>());
        UNIT_ASSERT_EQUAL(subscrsMap.Size(), 3);

        // different address
        subscrsMap.Subscribe(lo6, ch1, "jamel", Max<TSubscrMask>());
        UNIT_ASSERT_EQUAL(subscrsMap.Size(), 4);

        // different id
        subscrsMap.Subscribe(lo6, ch1, "taskId", Max<TSubscrMask>());
        UNIT_ASSERT_EQUAL(subscrsMap.Size(), 5);

        // same (addr, chId, id)s
        subscrsMap.Subscribe(lo4, ch1, "jamel", 0x1111);
        subscrsMap.Subscribe(lo4, ch2, "jamel", 0x3333);
        subscrsMap.Subscribe(lo6, ch1, "jamel", 0x5555);
        subscrsMap.Subscribe(lo6, ch1, "taskId", 0x7777);
        UNIT_ASSERT_EQUAL(subscrsMap.Size(), 5);
    }

    Y_UNIT_TEST(SubscribeIf) {
        NAddr::TIPv4Addr lo4(IpV4Loopback(12345));
        TChannelId ch1 = 0x76543210;
        TChannelId ch2 = ch1 + 1;

        TTaskSubscriptions subscrsMap;
        UNIT_ASSERT_EQUAL(subscrsMap.Size(), 0);

        bool subscribed = subscrsMap.SubscribeIf(
                    lo4, ch1, "jamel", Max<TSubscrMask>(),
                    []() { return true; });
        UNIT_ASSERT(subscribed);
        UNIT_ASSERT_EQUAL(subscrsMap.Size(), 1);

        subscribed = subscrsMap.SubscribeIf(
                    lo4, ch2, "jamel", Max<TSubscrMask>(),
                    []() { return false; });
        UNIT_ASSERT(!subscribed);
        UNIT_ASSERT_EQUAL(subscrsMap.Size(), 1);
    }

    Y_UNIT_TEST(Find) {
        NAddr::TIPv4Addr lo4(IpV4Loopback(12345));
        NAddr::TIPv6Addr lo6(IpV6Loopback(12345));
        TChannelId ch1 = 0x76543210;
        TChannelId ch2 = ch1 + 1;

        TTaskSubscriptions subscrsMap;
        subscrsMap.Subscribe(lo4, ch1, "jamel", 0x1111);

        {
            auto subscrs = subscrsMap.Find("jamel", 0x1);
            UNIT_ASSERT_EQUAL(subscrs.size(), 1);

            TTaskSubscriptionPtr s = subscrs[0];
            UNIT_ASSERT(NAddr::IsSame(s->GetAddress(), lo4));
            UNIT_ASSERT_EQUAL(s->GetChannelId(), ch1);
            UNIT_ASSERT_EQUAL(s->GetMask(), 0x1111);

            // unregistered mask
            subscrs = subscrsMap.Find("jamel", 0x2);
            UNIT_ASSERT_EQUAL(subscrs.size(), 0);
        }

        subscrsMap.Subscribe(lo4, ch2, "jamel", 0x5555);
        subscrsMap.Subscribe(lo4, ch1, "jamel", 0x6666);

        {
            auto subscrs = subscrsMap.Find("jamel", 0x1);
            UNIT_ASSERT_EQUAL(subscrs.size(), 1);

            TTaskSubscriptionPtr s = subscrs[0];
            UNIT_ASSERT(NAddr::IsSame(s->GetAddress(), lo4));
            UNIT_ASSERT_EQUAL(s->GetChannelId(), ch2);
            UNIT_ASSERT_EQUAL(s->GetMask(), 0x5555);

            subscrs = subscrsMap.Find("jamel", 0x2);
            UNIT_ASSERT_EQUAL(subscrs.size(), 1);

            s = subscrs[0];
            UNIT_ASSERT(NAddr::IsSame(s->GetAddress(), lo4));
            UNIT_ASSERT_EQUAL(s->GetChannelId(), ch1);
            UNIT_ASSERT_EQUAL(s->GetMask(), 0x6666);
        }

        subscrsMap.Subscribe(lo6, ch1, "jamel", 0x3333);
        {
            auto subscrs = subscrsMap.Find("jamel", 0x1);
            UNIT_ASSERT_EQUAL(subscrs.size(), 2);

            TTaskSubscriptionPtr s = subscrs[0];
            UNIT_ASSERT(NAddr::IsSame(s->GetAddress(), lo4));
            UNIT_ASSERT_EQUAL(s->GetChannelId(), ch2);
            UNIT_ASSERT_EQUAL(s->GetMask(), 0x5555);

            s = subscrs[1];
            UNIT_ASSERT(NAddr::IsSame(s->GetAddress(), lo6));
            UNIT_ASSERT_EQUAL(s->GetChannelId(), ch1);
            UNIT_ASSERT_EQUAL(s->GetMask(), 0x3333);
        }
    }

    Y_UNIT_TEST(UnsubscribeByAddress) {
        NAddr::TIPv4Addr lo4(IpV4Loopback(12345));
        NAddr::TIPv6Addr lo6(IpV6Loopback(12345));
        TChannelId ch1 = 0x76543210;
        TChannelId ch2 = ch1 + 1;

        TTaskSubscriptions subscrsMap;
        subscrsMap.Subscribe(lo4, ch1, "jamel", 0x1111);
        subscrsMap.Subscribe(lo4, ch2, "jamel", 0x2222);
        subscrsMap.Subscribe(lo6, ch1, "jamel", 0x3333);
        subscrsMap.Subscribe(lo6, ch2, "jamel", 0x4444);

        subscrsMap.Subscribe(lo4, ch1, "task1", 0x1111);
        subscrsMap.Subscribe(lo4, ch2, "task1", 0x2222);
        subscrsMap.Subscribe(lo6, ch1, "task1", 0x3333);
        subscrsMap.Subscribe(lo6, ch2, "task1", 0x4444);

        subscrsMap.Subscribe(lo4, ch1, "task2", 0x1111);
        subscrsMap.Subscribe(lo4, ch2, "task2", 0x2222);
        subscrsMap.Subscribe(lo6, ch1, "task2", 0x3333);
        subscrsMap.Subscribe(lo6, ch2, "task2", 0x4444);

        {
            UNIT_ASSERT_EQUAL(subscrsMap.Size(), 12);

            auto jamelSubscrs = subscrsMap.Find("jamel", Max<ui64>());
            UNIT_ASSERT_EQUAL(jamelSubscrs.size(), 4);

            auto task1Subscrs = subscrsMap.Find("task1", Max<ui64>());
            UNIT_ASSERT_EQUAL(task1Subscrs.size(), 4);

            auto task2Subscrs = subscrsMap.Find("task2", Max<ui64>());
            UNIT_ASSERT_EQUAL(task2Subscrs.size(), 4);
        }

        subscrsMap.Unsubscribe(lo4, ch1);

        {
            UNIT_ASSERT_EQUAL(subscrsMap.Size(), 9);

            auto lo6Ch2 = [&lo4, ch1](const TTaskSubscriptionPtr& s) {
                return NAddr::IsSame(s->GetAddress(), lo4) &&
                        s->GetChannelId() == ch1;
            };

            auto jamelSubscrs = subscrsMap.Find("jamel", Max<ui64>());
            UNIT_ASSERT_EQUAL(jamelSubscrs.size(), 3);
            UNIT_ASSERT(FindIf(jamelSubscrs, lo6Ch2) == jamelSubscrs.end());

            auto task1Subscrs = subscrsMap.Find("task1", Max<ui64>());
            UNIT_ASSERT_EQUAL(task1Subscrs.size(), 3);
            UNIT_ASSERT(FindIf(task1Subscrs, lo6Ch2) == task1Subscrs.end());

            auto task2Subscrs = subscrsMap.Find("task2", Max<ui64>());
            UNIT_ASSERT_EQUAL(task2Subscrs.size(), 3);
            UNIT_ASSERT(FindIf(task2Subscrs, lo6Ch2) == task2Subscrs.end());
        }

        subscrsMap.Unsubscribe(lo6, ch2);

        {
            UNIT_ASSERT_EQUAL(subscrsMap.Size(), 6);

            auto lo6Ch2 = [&lo6, ch2](const TTaskSubscriptionPtr& s) {
                return NAddr::IsSame(s->GetAddress(), lo6) &&
                        s->GetChannelId() == ch2;
            };

            auto jamelSubscrs = subscrsMap.Find("jamel", Max<ui64>());
            UNIT_ASSERT_EQUAL(jamelSubscrs.size(), 2);
            UNIT_ASSERT(FindIf(jamelSubscrs, lo6Ch2) == jamelSubscrs.end());

            auto task1Subscrs = subscrsMap.Find("task1", Max<ui64>());
            UNIT_ASSERT_EQUAL(task1Subscrs.size(), 2);
            UNIT_ASSERT(FindIf(task1Subscrs, lo6Ch2) == task1Subscrs.end());

            auto task2Subscrs = subscrsMap.Find("task2", Max<ui64>());
            UNIT_ASSERT_EQUAL(task2Subscrs.size(), 2);
            UNIT_ASSERT(FindIf(task2Subscrs, lo6Ch2) == task2Subscrs.end());
        }
    }

    Y_UNIT_TEST(UnsubscribeById) {
        NAddr::TIPv4Addr lo4(IpV4Loopback(12345));
        NAddr::TIPv6Addr lo6(IpV6Loopback(12345));
        TChannelId ch1 = 0x76543210;
        TChannelId ch2 = ch1 + 1;

        TTaskSubscriptions subscrsMap;
        subscrsMap.Subscribe(lo4, ch1, "jamel", 0x1111);
        subscrsMap.Subscribe(lo4, ch2, "jamel", 0x2222);
        subscrsMap.Subscribe(lo6, ch1, "jamel", 0x3333);
        subscrsMap.Subscribe(lo6, ch2, "jamel", 0x4444);

        subscrsMap.Subscribe(lo4, ch1, "task1", 0x1111);
        subscrsMap.Subscribe(lo4, ch2, "task1", 0x2222);
        subscrsMap.Subscribe(lo6, ch1, "task1", 0x3333);
        subscrsMap.Subscribe(lo6, ch2, "task1", 0x4444);

        subscrsMap.Subscribe(lo4, ch1, "task2", 0x1111);
        subscrsMap.Subscribe(lo4, ch2, "task2", 0x2222);
        subscrsMap.Subscribe(lo6, ch1, "task2", 0x3333);
        subscrsMap.Subscribe(lo6, ch2, "task2", 0x4444);

        {
            UNIT_ASSERT_EQUAL(subscrsMap.Size(), 12);

            auto jamelSubscrs = subscrsMap.Find("jamel", Max<ui64>());
            UNIT_ASSERT_EQUAL(jamelSubscrs.size(), 4);

            auto task1Subscrs = subscrsMap.Find("task1", Max<ui64>());
            UNIT_ASSERT_EQUAL(task1Subscrs.size(), 4);

            auto task2Subscrs = subscrsMap.Find("task2", Max<ui64>());
            UNIT_ASSERT_EQUAL(task2Subscrs.size(), 4);
        }

        {
            auto subscrs = subscrsMap.Unsubscribe("task1");
            UNIT_ASSERT_EQUAL(subscrs.size(), 4);
            UNIT_ASSERT_EQUAL(subscrsMap.Size(), 8);

            auto jamelSubscrs = subscrsMap.Find("jamel", Max<ui64>());
            UNIT_ASSERT_EQUAL(jamelSubscrs.size(), 4);

            auto task1Subscrs = subscrsMap.Find("task1", Max<ui64>());
            UNIT_ASSERT_EQUAL(task1Subscrs.size(), 0);

            auto task2Subscrs = subscrsMap.Find("task2", Max<ui64>());
            UNIT_ASSERT_EQUAL(task2Subscrs.size(), 4);
        }

        {
            auto subscrs = subscrsMap.Unsubscribe("jamel");
            UNIT_ASSERT_EQUAL(subscrs.size(), 4);
            UNIT_ASSERT_EQUAL(subscrsMap.Size(), 4);

            auto jamelSubscrs = subscrsMap.Find("jamel", Max<ui64>());
            UNIT_ASSERT_EQUAL(jamelSubscrs.size(), 0);

            auto task2Subscrs = subscrsMap.Find("task2", Max<ui64>());
            UNIT_ASSERT_EQUAL(task2Subscrs.size(), 4);
        }


        {
            auto subscrs = subscrsMap.Unsubscribe("task2");
            UNIT_ASSERT_EQUAL(subscrs.size(), 4);
            UNIT_ASSERT_EQUAL(subscrsMap.Size(), 0);

            auto task2Subscrs = subscrsMap.Find("task2", Max<ui64>());
            UNIT_ASSERT_EQUAL(task2Subscrs.size(), 0);
        }
    }
}
