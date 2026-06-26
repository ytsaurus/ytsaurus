// Bus benchmark: two workloads over the TCP transport.
//
//   pingpong   - request/reply latency: client sends a small message and waits
//                for the echo, N times; reports round-trip latency and RPS.
//   throughput - one-way bulk transfer: client keeps a fixed window of sends in
//                flight; both ends report throughput and process CPU (user/sys
//                via getrusage), so the headline metric is CPU per GB moved.
//
//   benchmark_bus --mode server --port 9100 [--workload throughput]
//   benchmark_bus --mode client --host HOST --port 9100 \
//       --workload throughput --size 4194304 --window 64 --seconds 12

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/client.h>
#include <yt/yt/core/bus/server.h>
#include <yt/yt/core/bus/message_handler.h>

#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/dispatcher.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/getopt/last_getopt.h>

#include <library/cpp/yt/string/format.h>

#include <util/generic/size_literals.h>

#include <util/stream/output.h>

#include <atomic>
#include <csignal>
#include <cstring>
#include <functional>
#include <memory>

#include <sys/resource.h>

namespace NYT::NBus {
namespace {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

constexpr double BytesPerGB = 1e9;
constexpr auto ServerReportPeriod = TDuration::Seconds(2);

const NLogging::TLogger Logger("BenchmarkBus");

struct TBenchmarkBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

struct TProcessCpuTimes
{
    TDuration User;
    TDuration System;
};

TProcessCpuTimes GetProcessCpuTimes()
{
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    auto toDuration = [] (const timeval& time) {
        return TDuration::Seconds(time.tv_sec) + TDuration::MicroSeconds(time.tv_usec);
    };
    return {
        .User = toDuration(usage.ru_utime),
        .System = toDuration(usage.ru_stime),
    };
}

////////////////////////////////////////////////////////////////////////////////

void ConfigureTransport(int threadCount)
{
    auto config = New<NTcp::TDispatcherConfig>();
    config->ThreadPoolSize = threadCount;
    NTcp::TDispatcher::Get()->Configure(config);
}

IBusServerPtr CreateServer(int port)
{
    return NTcp::CreateBusServer(NTcp::TBusServerConfig::CreateTcp(port));
}

IBusClientPtr CreateClient(const std::string& address)
{
    return NTcp::CreateBusClient(NTcp::TBusClientConfig::CreateTcp(address));
}

////////////////////////////////////////////////////////////////////////////////
// pingpong workload.

class TEchoServer
    : public IMessageHandler
{
public:
    void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus,
        IDirectPlacementTransferPtr /*transfer*/) noexcept override
    {
        YT_UNUSED_FUTURE(replyBus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full})
            .ToUncancelable());
    }
};

class TEchoReceiver
    : public IMessageHandler
{
public:
    void ResetPromise()
    {
        ReplyPromise_ = NewPromise<void>();
    }

    void Await()
    {
        WaitFor(ReplyPromise_.ToFuture())
            .ThrowOnError();
    }

    void HandleMessage(
        TSharedRefArray /*message*/,
        IBusPtr /*replyBus*/,
        IDirectPlacementTransferPtr /*transfer*/) noexcept override
    {
        ReplyPromise_.Set();
    }

private:
    TPromise<void> ReplyPromise_ = NewPromise<void>();
};

void RunPingPong(const std::string& address, int iterations)
{
    auto client = CreateClient(address);
    auto handler = New<TEchoReceiver>();
    auto bus = client->CreateBus(handler);
    WaitFor(bus->GetReadyFuture())
        .ThrowOnError();

    auto message = TSharedRefArray(TSharedRef::FromString("ping"));

    TWallTimer timer;
    for (int iteration = 0; iteration < iterations; ++iteration) {
        handler->ResetPromise();
        WaitFor(bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full}))
            .ThrowOnError();
        handler->Await();
    }
    auto wall = timer.GetElapsedTime();

    YT_LOG_INFO("Ping-pong result (Iterations: %v, WallTime: %.3fs, Latency: %.1fus, Throughput: %.0f req/s)",
        iterations,
        wall.SecondsFloat(),
        static_cast<double>(wall.MicroSeconds()) / iterations,
        iterations / wall.SecondsFloat());
}

////////////////////////////////////////////////////////////////////////////////
// throughput workload.

struct TCountingHandler
    : public IMessageHandler
{
    std::atomic<i64> Bytes = 0;
    std::atomic<i64> Messages = 0;

    void HandleMessage(
        TSharedRefArray message,
        IBusPtr /*replyBus*/,
        IDirectPlacementTransferPtr /*transfer*/) noexcept override
    {
        Bytes.fetch_add(message.ByteSize(), std::memory_order::relaxed);
        Messages.fetch_add(1, std::memory_order::relaxed);
    }
};

void RunThroughputClient(
    const std::string& address,
    i64 size,
    int window,
    TDuration warmupTime,
    TDuration measureTime)
{
    auto client = CreateClient(address);
    auto bus = client->CreateBus(New<TCountingHandler>());
    WaitFor(bus->GetReadyFuture())
        .ThrowOnError();

    // One reusable payload; the same refcounted message is sent by every in-flight
    // slot, so only one buffer is allocated regardless of window.
    auto buffer = TSharedMutableRef::Allocate<TBenchmarkBufferTag>(size, {.InitializeStorage = false});
    std::memset(buffer.Begin(), 0xAB, size);
    TSharedRefArray message{TSharedRef(buffer)};

    TSendOptions options{
        .TrackingLevel = EDeliveryTrackingLevel::Full,
    };

    YT_LOG_INFO("Throughput client started (Address: %v, Size: %v, Window: %v)",
        address,
        size,
        window);

    // Continuous sliding window: keep #window sends outstanding; each slot re-arms
    // itself from its own completion so the link stays full.
    std::atomic<i64> sentBytes = 0;
    std::atomic<bool> running = true;
    std::atomic<int> activeSlots = window;
    auto allDone = NewPromise<void>();

    auto pump = std::make_shared<std::function<void()>>();
    *pump = [&, pump] {
        if (!running.load(std::memory_order::relaxed)) {
            if (activeSlots.fetch_sub(1) == 1) {
                allDone.Set();
            }
            return;
        }
        bus->Send(message, options).Subscribe(BIND([&, pump] (const TError& error) {
            if (error.IsOK()) {
                sentBytes.fetch_add(size, std::memory_order::relaxed);
            } else {
                running.store(false, std::memory_order::relaxed);
            }
            (*pump)();
        }));
    };
    for (int index = 0; index < window; ++index) {
        (*pump)();
    }

    auto sleepFor = [] (TDuration duration) {
        WaitFor(TDelayedExecutor::MakeDelayed(duration))
            .ThrowOnError();
    };

    sleepFor(warmupTime);
    auto cpu0 = GetProcessCpuTimes();
    TWallTimer timer;
    i64 bytes0 = sentBytes.load();
    sleepFor(measureTime);
    auto cpu1 = GetProcessCpuTimes();
    auto wall = timer.GetElapsedTime();
    i64 bytes = sentBytes.load() - bytes0;

    running.store(false, std::memory_order::relaxed);
    WaitFor(allDone.ToFuture())
        .ThrowOnError();

    double wallSeconds = wall.SecondsFloat();
    double gb = bytes / BytesPerGB;
    double user = (cpu1.User - cpu0.User).SecondsFloat();
    double sys = (cpu1.System - cpu0.System).SecondsFloat();
    double cpu = user + sys;
    double gbps = gb / wallSeconds;

    YT_LOG_INFO("Throughput result (PayloadSize: %.3f MiB, Window: %v, WallTime: %.2fs, "
        "DataMoved: %.2f GB, Throughput: %.2f GB/s, "
        "ClientCpu: %.2f cores (User: %.2f, Sys: %.2f), "
        "CoresPerGBps: user %.4f sys %.4f total %.4f)",
        size / static_cast<double>(1_MB),
        window,
        wallSeconds,
        gb,
        gbps,
        cpu / wallSeconds, user / wallSeconds, sys / wallSeconds,
        (user / wallSeconds) / gbps, (sys / wallSeconds) / gbps, (cpu / wallSeconds) / gbps);
}

////////////////////////////////////////////////////////////////////////////////

volatile std::sig_atomic_t Stopped = 0;

void RunThroughputServer(const TIntrusivePtr<TCountingHandler>& handler)
{
    TWallTimer timer;
    auto prevElapsed = TDuration::Zero();
    auto prevCpu = GetProcessCpuTimes();
    i64 prevBytes = 0;
    i64 prevMessages = 0;
    while (!Stopped) {
        Sleep(ServerReportPeriod);
        auto elapsed = timer.GetElapsedTime();
        auto cpu = GetProcessCpuTimes();
        i64 bytes = handler->Bytes.load();
        i64 messages = handler->Messages.load();

        double dt = (elapsed - prevElapsed).SecondsFloat();
        double gb = (bytes - prevBytes) / BytesPerGB;
        double user = (cpu.User - prevCpu.User).SecondsFloat();
        double sys = (cpu.System - prevCpu.System).SecondsFloat();
        if (gb > 1e-9) {
            double gbps = gb / dt;
            YT_LOG_INFO("Server throughput (Throughput: %.2f GB/s, MessageRate: %.0f msg/s, "
                "Cpu: %.2f cores (User: %.2f, Sys: %.2f), "
                "CoresPerGBps: user %.4f sys %.4f total %.4f)",
                gbps, (messages - prevMessages) / dt,
                (user + sys) / dt, user / dt, sys / dt,
                (user / dt) / gbps, (sys / dt) / gbps, ((user + sys) / dt) / gbps);
        }
        prevElapsed = elapsed;
        prevCpu = cpu;
        prevBytes = bytes;
        prevMessages = messages;
    }
}

void RunServer(int port, bool throughput)
{
    if (throughput) {
        auto handler = New<TCountingHandler>();
        auto server = CreateServer(port);
        server->Start(handler);
        YT_LOG_INFO("Throughput server started (Port: %v)", port);
        RunThroughputServer(handler);
    } else {
        auto server = CreateServer(port);
        server->Start(New<TEchoServer>());
        YT_LOG_INFO("Echo server started (Port: %v)", port);
        TDelayedExecutor::WaitForDuration(TDuration::Max());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NBus

int main(int argc, char* argv[])
{
    using namespace NYT;
    using namespace NYT::NBus;
    using namespace NYT::NConcurrency;
    using namespace NLastGetopt;

    try {
        TOpts opts;

        std::string mode;
        opts.AddLongOption("mode", "server | client")
            .RequiredArgument("MODE")
            .StoreResult(&mode);

        std::string workload = "pingpong";
        opts.AddLongOption("workload", "pingpong | throughput")
            .OptionalArgument("WORKLOAD")
            .StoreResult(&workload);

        int port = 0;
        opts.AddLongOption("port", "")
            .RequiredArgument("PORT")
            .StoreResult(&port);

        std::string host = "localhost";
        opts.AddLongOption("host", "client target host")
            .OptionalArgument("HOST")
            .StoreResult(&host);

        int iterations = 10000;
        opts.AddLongOption("iterations", "pingpong round trips")
            .OptionalArgument("N")
            .StoreResult(&iterations);

        i64 size = 4_MBs;
        opts.AddLongOption("size", "throughput payload bytes")
            .OptionalArgument("BYTES")
            .StoreResult(&size);

        int window = 64;
        opts.AddLongOption("window", "throughput in-flight sends")
            .OptionalArgument("N")
            .StoreResult(&window);

        double warmup = 3;
        opts.AddLongOption("warmup", "throughput warmup seconds")
            .OptionalArgument("SEC")
            .StoreResult(&warmup);

        double seconds = 12;
        opts.AddLongOption("seconds", "throughput measure seconds")
            .OptionalArgument("SEC")
            .StoreResult(&seconds);

        int threads = 4;
        opts.AddLongOption("threads", "worker threads")
            .OptionalArgument("N")
            .StoreResult(&threads);

        TOptsParseResult results(&opts, argc, argv);

        bool throughput = (workload == "throughput");

        ConfigureTransport(threads);

        if (mode == "server") {
            std::signal(SIGINT, [] (int) { Stopped = 1; });
            std::signal(SIGTERM, [] (int) { Stopped = 1; });
            RunServer(port, throughput);
            return 0;
        }

        if (mode == "client") {
            auto address = Format("%v:%v", host, port);
            // WaitFor needs a fiber: run the client body on an action queue and block.
            auto queue = New<TActionQueue>("Client");
            TFuture<void> result;
            if (throughput) {
                result = BIND(&RunThroughputClient, address, size, window, TDuration::Seconds(warmup), TDuration::Seconds(seconds))
                    .AsyncVia(queue->GetInvoker())
                    .Run();
            } else {
                result = BIND(&RunPingPong, address, iterations)
                    .AsyncVia(queue->GetInvoker())
                    .Run();
            }
            result.BlockingGet().ThrowOnError();
            return 0;
        }

        THROW_ERROR_EXCEPTION("Unknown mode %Qv", mode);
    } catch (const std::exception& ex) {
        Cerr << "Error: " << ex.what() << Endl;
        return 1;
    }
}
