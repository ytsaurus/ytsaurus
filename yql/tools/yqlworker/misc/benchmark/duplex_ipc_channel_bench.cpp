#include <yql/tools/yqlworker/misc/duplex_ipc_channel.h>
#include <yql/tools/yqlworker/misc/test_support/test.pb.h>

#include <benchmark/benchmark.h>

#include <util/generic/scope.h>
#include <util/generic/yexception.h>

#include <thread>

namespace {

    void Respond(NYql::TIpcChannelEndpoint channel) {
        try {
            for (;;) {
                TSlimMsg message;
                channel.RecvMsg(&message);
                message.SetInt32(message.GetInt32() + 1);
                channel.SendMsg(message);
            }
        } catch (const NYql::TPeerDead&) {
        }
    }

    void DuplexRoundTrips(benchmark::State& state) {
        auto [sender, receiver] = NYql::DuplexIpcChannel();
        std::thread incrementer(Respond, std::move(receiver));
        Y_DEFER {
            sender.Close();
#ifdef _linux_
            incrementer.join();
#else
            // Datagram recv may remain blocked after the peer closes on Darwin.
            incrementer.detach();
#endif
        };

        TSlimMsg request;
        TSlimMsg response;
        ui32 index = 0;
        for ([[maybe_unused]] auto iteration : state) {
            request.SetInt32(index);
            sender.SendMsg(request);
            sender.RecvMsg(&response);
            Y_ENSURE(response.GetInt32() == index + 1, "Unexpected response: " << response.GetInt32() << ", request: " << index);
            ++index;
        }
    }

    BENCHMARK(DuplexRoundTrips)->UseRealTime();

} // namespace
