#include "duplex_ipc_channel.h"

#include <yql/tools/yqlworker/misc/test.pb.h>

#include <yql/essentials/utils/signals/utils.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>
#include <util/generic/scope.h>

#include <thread>


using namespace NYql;

Y_UNIT_TEST_SUITE(TDuplexChannelTest) {

    Y_UNIT_TEST(UnidirectionalSendReceive) {
        TIpcChannelEndpoint ep1, ep2;
        std::tie(ep1, ep2) = DuplexIpcChannel();

        TSlimMsg slimMsg;
        slimMsg.SetInt32(42);
        slimMsg.SetFloat(2.718);

        TFatMsg fatMsg;
        fatMsg.SetStr("quick brown fox jumps over the lazy dog");
        fatMsg.MutableSubMsg()->CopyFrom(slimMsg);
        for (ui32 i = 0; i < 100; i++) {
            fatMsg.AddInts(i);
        }

        { // slim message
            TSlimMsg msg;

            ep1.SendMsg(slimMsg);
            ep2.RecvMsg(&msg);

            UNIT_ASSERT_EQUAL(msg.GetInt32(), 42);
            UNIT_ASSERT_DOUBLES_EQUAL(msg.GetFloat(), 2.718, 0.0001);
        }

        { // fat message
            TFatMsg msg;

            ep1.SendMsg(fatMsg);
            ep2.RecvMsg(&msg);

            UNIT_ASSERT_STRINGS_EQUAL(
                        msg.GetStr(),
                        "quick brown fox jumps over the lazy dog");

            const TSlimMsg& subMsg = msg.GetSubMsg();
            UNIT_ASSERT_EQUAL(subMsg.GetInt32(), 42);
            UNIT_ASSERT_DOUBLES_EQUAL(subMsg.GetFloat(), 2.718, 0.0001);

            UNIT_ASSERT_EQUAL(msg.IntsSize(), 100);
            for (ui32 i = 0; i < 100; i++) {
                UNIT_ASSERT_EQUAL(msg.GetInts(i), i);
            }
        }
    }

    Y_UNIT_TEST(BidirectionalSendReceive) {
        TIpcChannelEndpoint ep1, ep2;
        std::tie(ep1, ep2) = DuplexIpcChannel();

        std::thread incrementer([&ep2]() {
            TSlimMsg req;
            ep2.RecvMsg(&req);

            TFatMsg resp;
            resp.MutableSubMsg()->SetInt32(req.GetInt32() + 1);
            resp.SetStr("response message");
            ep2.SendMsg(resp);
        });

        Y_DEFER {
            incrementer.join();
        };

        {
            TSlimMsg req;
            req.SetInt32(42);
            ep1.SendMsg(req);

            TFatMsg resp;
            ep1.RecvMsg(&resp);
            UNIT_ASSERT_STRINGS_EQUAL(resp.GetStr(), "response message");
            UNIT_ASSERT_EQUAL(resp.GetSubMsg().GetInt32(), 43);
        }
    }

    Y_UNIT_TEST(PeerDeadOnSend) {
        TIpcChannelEndpoint ep1, ep2;
        std::tie(ep1, ep2) = DuplexIpcChannel();
        ep2.Close();

        TSlimMsg msg;
        msg.SetInt32(42);
        UNIT_ASSERT_EXCEPTION(ep1.SendMsg(msg), TPeerDead);
    }

    Y_UNIT_TEST(PeerDeadOnReceive) {
        TIpcChannelEndpoint ep1, ep2;
        std::tie(ep1, ep2) = DuplexIpcChannel();

        std::thread reader([&ep2]() {
            TSlimMsg req;
            ep2.RecvMsg(&req); // used for synchronization
            UNIT_ASSERT_EXCEPTION(ep2.RecvMsg(&req), TPeerDead);
        });

        Y_DEFER {
            reader.join();
        };

        TSlimMsg req;
        req.SetInt32(42);
        ep1.SendMsg(req);
        ep1.Close();
    }

    Y_UNIT_TEST(NonBlockSendReceive) {
        TIpcChannelEndpoint ep1, ep2;
        std::tie(ep1, ep2) = DuplexIpcChannel();

        { // nothing in the socket yet
            TSlimMsg req;
            UNIT_ASSERT(false == ep2.RecvMsgNonBlock(&req));
        }

        {
            TSlimMsg req;
            req.SetInt32(42);
            UNIT_ASSERT(ep1.SendMsgNonBlock(req));
        }

        {
            TSlimMsg req;
            UNIT_ASSERT(ep2.RecvMsgNonBlock(&req));

            TFatMsg resp;
            resp.MutableSubMsg()->CopyFrom(req);
            resp.SetStr("response message");
            UNIT_ASSERT(ep2.SendMsgNonBlock(resp));
        }

        {
            TFatMsg resp;
            UNIT_ASSERT(ep1.RecvMsgNonBlock(&resp));
            UNIT_ASSERT_EQUAL(resp.GetSubMsg().GetInt32(), 42);
            UNIT_ASSERT_STRINGS_EQUAL(resp.GetStr(), "response message");
        }
    }

    Y_UNIT_TEST(Perf) {
        TIpcChannelEndpoint ep1, ep2;
        std::tie(ep1, ep2) = DuplexIpcChannel();

        // make the same lifetime of channel endpoint as lifetime of this thread
        std::thread incrementer([ch = std::move(ep2)]() mutable {
            try {
                for (;;) {
                    TSlimMsg msg;
                    ch.RecvMsg(&msg);
                    msg.SetInt32(msg.GetInt32() + 1);
                    ch.SendMsg(msg);
                }
            } catch (const TPeerDead& e) {
                // just stop iteration
            }
        });

        Y_DEFER {
            ep1.Close();
#ifdef _linux_
            incrementer.join();
#else
            // For non linux (e.g. maxOS) systems DuplexChannelPair() creates
            // SOCK_DGRAM socket which can lead reader to hangup on recv() from
            // socket. So here we datach reading thread to avoid this issue.
            incrementer.detach();
#endif
        };

        {
            TSlimMsg req, resp;
            TSimpleTimer timer;

            for (ui32 i = 0; i < 10000; i++) {
                req.SetInt32(i);
                ep1.SendMsg(req);
                ep1.RecvMsg(&resp);
                UNIT_ASSERT_EQUAL(resp.GetInt32(), i + 1);
            }

            TDuration elapsed = timer.Get();
            Cerr << "elapsed: " << elapsed << Endl;
#ifndef WITH_VALGRIND
            UNIT_ASSERT(elapsed < TDuration::Seconds(10));
#endif
            ep1.Close();
        }
    }
}
