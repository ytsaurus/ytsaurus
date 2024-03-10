#include <yt/yt/library/program/program.h>

#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/formatter.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/misc/blob_output.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/mpsc_stack.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/concurrency/async_semaphore.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/scheduler_thread.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/folder/pathsplit.h>
#include <util/random/fast.h>
#include <util/stream/printf.h>

#include <thread>

using namespace NYT::NYson;
using namespace NYT::NYTree;
using namespace NYT::NConcurrency;
using namespace NYT::NProfiling;
using namespace NYT::NApi;
using namespace NYT::NApi::NRpcProxy;
using namespace NYT::NTableClient;
using namespace NYT::NCypressClient;
using namespace NLastGetopt;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDriverType,
    (RpcProxy)
    (Native)
);

class TTestConfig
    : public TYsonStructLite
{
public:
    TString User;
    TString Token;
    TString Table;

    int MaxInflight;
    int MaxRps;
    int Threads;
    int Clients;
    int PacketSize;
    int MaxInserts;
    int MaxTables;
    TDuration PrintInterval;

    NNet::TAddressResolverConfigPtr AddressResolver;
    NRpc::TDispatcherConfigPtr RpcDispatcher;
    NLogging::TLogManagerConfigPtr Logging;
    NRpcProxy::TConnectionConfigPtr Connection;

    NApi::NNative::TConnectionCompoundConfigPtr Driver;

    EDriverType DriverType;

    REGISTER_YSON_STRUCT_LITE(TTestConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("user", &TThis::User);
        registrar.Parameter("token", &TThis::Token);

        registrar.Parameter("table", &TThis::Table)
            .Default("//home/stress-test-insert-3");

        registrar.Parameter("max_inflight", &TThis::MaxInflight)
            .Default(100);
        registrar.Parameter("max_rps", &TThis::MaxRps)
            .Default(500);
        registrar.Parameter("max_packet", &TThis::PacketSize)
            .Default(10);
        registrar.Parameter("max_inserts", &TThis::MaxInserts)
            .Default(10);
        registrar.Parameter("max_tables", &TThis::MaxTables)
            .Default(10);
        registrar.Parameter("threads", &TThis::Threads)
            .Default(16);
        registrar.Parameter("clients", &TThis::Clients)
            .Default(16);
        registrar.Parameter("print_interval", &TThis::PrintInterval)
            .Default(TDuration::Seconds(1));

        registrar.Parameter("address_resolver", &TThis::AddressResolver)
            .DefaultNew();
        registrar.Parameter("rpc_dispatcher", &TThis::RpcDispatcher)
            .DefaultNew();
        registrar.Parameter("logging", &TThis::Logging)
            .DefaultCtor([] () { return NLogging::TLogManagerConfig::CreateSilent(); });
        registrar.Parameter("connection", &TThis::Connection)
            .DefaultNew();

        registrar.Parameter("driver", &TThis::Driver)
            .DefaultNew();

        registrar.Parameter("driver_type", &TThis::DriverType)
            .Default(EDriverType::RpcProxy);
    }
};

struct TAmmo
{
    TYPath Path;
    NTableClient::TRowBufferPtr RowBuffer;
    TSharedRange<NTableClient::TUnversionedRow> Rows;
};

struct TShot
{
    TCpuInstant Start;
    TCpuDuration Duration;
    i64 Size;
    NTransactionClient::TTransactionId TxId;
};

class TBuckets
{
public:
    TBuckets()
        : LastTick_(TInstant::Now().MilliSeconds() * 256)
    { }

    void Update(const TShot& shot)
    {
        auto guard = Guard(MeasurementsLock_);
        BytesWritten_ += shot.Size;
        Measurements_.emplace_back(GetTick(), shot);
        CleanUp();
    }

    void Print(const TCpuInstant& now)
    {
        std::optional<TShot> lastShot;

        {
            auto guard = Guard(MeasurementsLock_);
            Timings_.reserve(Measurements_.size());
            Timings_.clear();
            Timings_.insert(Timings_.end(), Measurements_.begin(), Measurements_.end());

            if (!Measurements_.empty()) {
                lastShot = std::optional(std::get<1>(Measurements_.front()));
            }
        }

        std::sort(Timings_.begin(), Timings_.end(),
            BIND([] (
                const std::tuple<ui64, TShot>& a,
                const std::tuple<ui64, TShot>& b) -> bool
            {
                return std::get<1>(a).Duration < std::get<1>(b).Duration;
            }));

        ui64 duration = 0;
        if (lastShot) {
            duration = CpuDurationToDuration(now - lastShot->Start).Seconds();
        }

        Cout << BytesWritten_ / 1024 / 1024 << "MiB, ";
        if (duration) {
            Cout << BytesWritten_ / duration / 1024 / 1024 << "MiB/s, ";
        } else {
            Cout << 0 << "MiB/s, ";
        }

        for (auto quant : Quantiles_) {
            int index = static_cast<int>(Timings_.size() * quant) - 1;
            auto result = index < 0 ? 0 : CpuDurationToDuration(std::get<1>(Timings_[index]).Duration).MilliSeconds();
            Printf(Cout, "%0.3lf%%=%ld, ", quant, result);
        }

        if (!Timings_.empty()) {
            const auto& last = Timings_.back();
            const auto& shot = std::get<1>(last);
            Cout << ToString(shot.TxId) << ", ";

            TRawFormatter<64> dateTimeBuffer;
            DateFormatter_.Format(&dateTimeBuffer, CpuInstantToInstant(shot.Start));
            Cout << dateTimeBuffer.GetBuffer() << ", ";
        }
    }

private:
    std::vector<double> Quantiles_ = {0.75, 0.95, 0.99, 0.995, 0.999, 1.0};
    std::vector<std::tuple<ui64, TShot>> Timings_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, MeasurementsLock_);
    std::list<std::tuple<ui64, TShot>> Measurements_;
    std::atomic<ui64> LastTick_;
    size_t BytesWritten_ = 0;

    NLogging::TCachingDateFormatter DateFormatter_;

    void CleanUp() {
        const auto window = (TInstant::Now().MilliSeconds() - 10*1000) * 256;

        auto it = Measurements_.begin();
        while (it != Measurements_.end()) {
            if (std::get<0>(*it) < window) {
                BytesWritten_ -= std::get<1>(*it).Size;
                Measurements_.erase(it++);
            } else {
                break;
            }
        }
    }

    ui64 GetTick() {
        for (;;) {
            ui64 oldTick = LastTick_;
            ui64 tick = TInstant::Now().MilliSeconds() * 256;
            ui64 newTick = tick - oldTick > 0 ? tick : oldTick + 1;
            if (LastTick_.compare_exchange_strong(oldTick, newTick)) {
                return newTick;
            }
        }
    }
};

struct IGaugePrinter
{
    virtual ~IGaugePrinter() = default;
    virtual void PrintMetrics() const = 0;
};

class TCollectorThread
    : public TSchedulerThread
{
public:
    TCollectorThread(
        TIntrusivePtr<NThreading::TEventCount> callbackEventCount,
        TMpscStack<TShot>& queue,
        TBuckets& writeBuckets)
        : TSchedulerThread(
            callbackEventCount,
            "ResultProcessing",
            {})
        , Queue_(queue)
        , WriteBuckets_(writeBuckets)
    { }

private:
    TMpscStack<TShot>& Queue_;

    TBuckets& WriteBuckets_;

    TClosure BeginExecute() override
    {
        if (Queue_.IsEmpty()) {
            return TClosure();
        }

        return BIND([this] {
            while (Queue_.DequeueAll(true, [&] (const TShot& shot) {
                UpdateStatistics(shot);
            }))
            { }
        });
    }

    void EndExecute() override
    { }

    void UpdateStatistics(const TShot& shot)
    {
        WriteBuckets_.Update(shot);
    }
};

class TStatPrinterThread
    : public TSchedulerThread
{

public:
    TStatPrinterThread(TBuckets& writeBuckets, const TDuration& printInterval, const IGaugePrinter* gaugePrinter)
        : TSchedulerThread(
            New<NThreading::TEventCount>(),
            "ResultPrinter",
            {})
        , PrintInterval_(printInterval)
        , WriteBuckets_(writeBuckets)
        , GaugePrinter_(gaugePrinter)
    { }

    TClosure BeginExecute() override
    {
        return BIND([this] {
            auto t0 = GetCpuInstant();

            PrintStastics(t0);

            auto t1 = GetCpuInstant();

            auto wait = PrintInterval_ - CpuDurationToDuration(t1 - t0);
            if (wait.NanoSeconds() > 0) {
                Sleep(wait);
            }
        });
    }

    void EndExecute() override
    { }

private:
    TDuration PrintInterval_;
    TBuckets& WriteBuckets_;
    const IGaugePrinter* GaugePrinter_;

    NLogging::TCachingDateFormatter DateFormatter_;

    void PrintStastics(TCpuInstant now)
    {
        TRawFormatter<64> dateTimeBuffer;
        DateFormatter_.Format(&dateTimeBuffer, CpuInstantToInstant(now));
        Cout << dateTimeBuffer.GetBuffer() << ", ";

        WriteBuckets_.Print(now);

        GaugePrinter_->PrintMetrics();

        Cout << Endl;
    }
};

class TTask
    : public IGaugePrinter
{
public:
    TTask(const TTestConfig& config, bool removeTables, bool createTables)
        : Config_(config)
        , RNG_(31337)
        , WorkerPool_(NConcurrency::CreateThreadPool(Config_.Threads, "benchmark_insert"))
        , ResultProcessingThread_(New<TCollectorThread>(
            ShootResultEventCount_,
            ShootResultQueue_,
            WriteBuckets_))
        , ResultPrintingThread_(New<TStatPrinterThread>(WriteBuckets_, Config_.PrintInterval, this))
        , NameTable_(New<TNameTable>())
    {
        NLogging::TLogManager::Get()->Configure(Config_.Logging);

        NNet::TAddressResolver::Get()->Configure(Config_.AddressResolver);

        NRpc::TDispatcher::Get()->Configure(Config_.RpcDispatcher);

        NameTable_->RegisterName("Data1");
        NameTable_->RegisterName("Data2");
        NameTable_->RegisterName("Data3");
        NameTable_->RegisterName("Data4");

        int maxPackets = 1000;
        Packets_.resize(maxPackets);
        for (int i = 0; i < maxPackets; ++i) {
            Packets_[i].resize(Config_.PacketSize);
            for (auto& ammo : Packets_[i]) {
                GenerateAmmo(&ammo);
            }
        }

        Clients_.reserve(Config_.Clients);
        for (int i = 0; i < Config_.Clients; ++i) {
            Clients_.push_back(CreateClient());
        }

        YT_VERIFY(!Clients_.empty());
        auto client = Clients_[0];

        std::vector<TFuture<void>> futures;

        if (removeTables) {
            for (int i = 0; i < Config_.MaxTables; ++i) {
                auto table = Format("%v-%v", Config_.Table, i);

                futures.push_back(client->RemoveNode(table));
            }

            WaitFor(AllSucceeded(futures))
                .ThrowOnError();

            futures.clear();
        }

        if (createTables) {
            for (int i = 0; i < Config_.MaxTables; ++i) {
                auto table = Format("%v-%v", Config_.Table, i);

                auto attributes = BuildYsonNodeFluently()
                    .BeginMap()
                        .Item("dynamic").Value(true)
                        .Item("schema")
                            .BeginList()
                                .Item()
                                    .BeginMap()
                                        .Item("name").Value("Data1")
                                        .Item("type").Value("string")
                                        .Item("sort_order").Value("ascending")
                                    .EndMap()
                                .Item()
                                    .BeginMap()
                                        .Item("name").Value("Data2")
                                        .Item("type").Value("string")
                                        .Item("sort_order").Value("ascending")
                                    .EndMap()
                                .Item()
                                    .BeginMap()
                                        .Item("name").Value("Data3")
                                        .Item("type").Value("string")
                                    .EndMap()
                                .Item()
                                    .BeginMap()
                                        .Item("name").Value("Data4")
                                        .Item("type").Value("string")
                                    .EndMap()
                            .EndList()
                    .EndMap();
                TCreateNodeOptions options;
                options.Attributes = ConvertToAttributes(attributes);

                futures.push_back(client->CreateNode(table, EObjectType::Table, options).As<void>());
            }

            WaitFor(AllSucceeded(futures))
                .ThrowOnError();

            futures.clear();

            for (int i = 0; i < Config_.MaxTables; ++i) {
                auto table = Format("%v-%v", Config_.Table, i);

                futures.push_back(client->MountTable(table));
            }

            WaitFor(AllSucceeded(futures))
                .ThrowOnError();

            futures.clear();

            for (int i = 0; i < Config_.MaxTables; ++i) {
                auto table = Format("%v-%v", Config_.Table, i);

                for (int attempt = 0; attempt < 100; ++attempt) {
                    auto value = WaitFor(client->GetNode(table + "/@tablet_state"))
                        .ValueOrThrow();
                    if (ConvertTo<IStringNodePtr>(value)->GetValue() == "mounted") {
                        break;
                    }
                    Sleep(TDuration::MilliSeconds(500));
                }
            }
        }

        auto tableMountCache = client->GetTableMountCache();

        for (int i = 0; i < Config_.MaxTables; ++i) {
            auto path = Format("%v-%v", Config_.Table, i);

            futures.push_back(tableMountCache->GetTableInfo(path).As<void>());
        }

        WaitFor(AllSucceeded(futures))
            .ThrowOnError();

        futures.clear();

        ResultProcessingThread_->Start();
        ResultPrintingThread_->Start();
    }

    void Run()
    {
        Loop();
    }

    void Shutdown()
    {
        WorkerPool_->Shutdown();
        ResultProcessingThread_->Stop();
        ResultPrintingThread_->Stop();
    }

private:
    const TTestConfig& Config_;
    TFastRng64 RNG_;

    IThreadPoolPtr WorkerPool_;

    TIntrusivePtr<NThreading::TEventCount> ShootResultEventCount_ = New<NThreading::TEventCount>();
    TMpscStack<TShot> ShootResultQueue_;

    TBuckets WriteBuckets_;
    TSchedulerThreadPtr ResultProcessingThread_;
    TSchedulerThreadPtr ResultPrintingThread_;

    // TODO: multiple files
    TString FileName_;
    std::shared_ptr<TFileHandle> File_;

    std::atomic<int> Inflight_ = {0};

    TNameTablePtr NameTable_;

    std::vector<std::vector<TAmmo>> Packets_;
    std::vector<IClientPtr> Clients_;

    IClientPtr CreateClient()
    {
        IClientPtr client;
        auto clientOptions = NApi::TClientOptions();
        if (Config_.User) {
            clientOptions.User = Config_.User;
        }
        if (Config_.Token) {
            clientOptions.Token = Config_.Token;
        }

        switch (Config_.DriverType) {
            case EDriverType::RpcProxy: {
                auto connection = NApi::NRpcProxy::CreateConnection(Config_.Connection);
                client = connection->CreateClient(clientOptions);
                break;
            }
            case EDriverType::Native: {
                auto connection = NApi::NNative::CreateConnection(Config_.Driver);
                client = connection->CreateClient(clientOptions);
                break;
            }
            default: {
                YT_ABORT();
            }
        }

        return client;
    }

    void PrintMetrics() const override
    {
        Cout << "fl=" << Inflight_.load(std::memory_order::relaxed) << ", ";
    }

    TString RandomString(int size) {
        TBlobOutput output;
        int total = (size + sizeof(ui64) - 1) / sizeof(ui64);
        for (int i = 0; i < total; ++i) {
            output << RNG_.GenRand();
        }
        return TString(output.Blob().Begin(), output.Blob().Size());
    }

    void GenerateAmmo(TAmmo* ammo)
    {
        int inserts = RNG_.GenRand() % Config_.MaxInserts + 1;
        ammo->RowBuffer = New<TRowBuffer>();
        ammo->Path = Format("%v-%v", Config_.Table, RNG_.GenRand() % Config_.MaxTables);

        std::vector<TUnversionedRow> rows;
        rows.reserve(inserts);

        for (int j = 0; j < inserts; ++j) {
            TUnversionedOwningRowBuilder builder;

            builder.AddValue(MakeUnversionedStringValue(RandomString(128), 0)); // Data1
            builder.AddValue(MakeUnversionedStringValue(RandomString(128), 1)); // Data2
            builder.AddValue(MakeUnversionedStringValue(RandomString(128), 2)); // Data3
            builder.AddValue(MakeUnversionedStringValue(RandomString(128), 3)); // Data4

            rows.push_back(ammo->RowBuffer->CaptureRow(builder.FinishRow()));
        }

        ammo->Rows = MakeSharedRange(std::move(rows), ammo->RowBuffer);
    }

    template<typename T>
    void CheckError(const TErrorOr<T>& errorOr) {
        if (!errorOr.IsOK()) {
            Cout << Format("%v", errorOr) << Endl;
            _exit(1);
        }
    }

    void Shoot(const IClientPtr& client, std::vector<TAmmo> packet)
    {
        auto t = GetCpuInstant();
        NTransactionClient::TTransactionId txId;
        auto transactionOr = WaitFor(client->StartTransaction(NTransactionClient::ETransactionType::Tablet));
        CheckError(transactionOr);

        auto tx = transactionOr.ValueOrThrow();
        txId = tx->GetId();

        for (const auto& ammo : packet) {
            SingleShoot(tx, ammo);
        }

        const auto& errorOr = WaitFor(tx->Commit());
        CheckError(errorOr);

        size_t size = 0;
        for (const auto& ammo : packet) {
            size += ammo.RowBuffer->GetSize();
        }

        TShot result;
        result.Size = size;
        result.Start = t;
        result.Duration = GetCpuInstant() - t;
        result.TxId = txId;
        ShootResultQueue_.Enqueue(result);
        ShootResultEventCount_->NotifyOne();

        -- Inflight_;
    }

    void SingleShoot(const NApi::ITransactionPtr& tx, const TAmmo& ammo)
    {
        tx->WriteRows(ammo.Path, NameTable_, ammo.Rows);
    }

    void Loop()
    {
        int currentPacket = 0;
        int currentClient = 0;
        auto baseWait = TDuration::MicroSeconds(1000000 / Config_.MaxRps);

        while (true) {
            auto t0 = GetCpuInstant();
            const auto& packet = Packets_[currentPacket];
            currentPacket = (currentPacket + 1) % Packets_.size();
            auto& client = Clients_[currentClient];
            currentClient = (currentClient + 1) % Clients_.size();

            ++ Inflight_;
            YT_UNUSED_FUTURE(BIND(&TTask::Shoot, this, client, packet)
                .AsyncVia(WorkerPool_->GetInvoker())
                .Run());

            while (Inflight_ > Config_.MaxInflight) {
                std::this_thread::yield();
            }

            auto t1 = GetCpuInstant();

            auto wait = baseWait - CpuDurationToDuration(t1 - t0);
            if (wait.NanoSeconds() > 0) {
                Sleep(wait);
            }
        }
    }
};

void RunTest(const TTestConfig& config, bool removeTables, bool createTables) {
    TTask test(config, removeTables, createTables);
    test.Run();
    test.Shutdown();
    Cout <<  "OK\n";
}

void GuardedMain(int argc, char** argv) {
    TOpts opts;
    TString configFileName;
    TTestConfig config;
    bool createTables = false;
    bool removeTables = false;

    opts.AddLongOption("config", "Config")
        .Required()
        .StoreResult(&configFileName);

    opts.AddLongOption("remove")
        .Optional()
        .NoArgument()
        .SetFlag(&removeTables);

    opts.AddLongOption("create")
        .Optional()
        .NoArgument()
        .SetFlag(&createTables);

    TOptsParseResult results(&opts, argc, argv);

    auto node = ConvertTo<INodePtr>(TYsonString(TUnbufferedFileInput(configFileName).ReadAll()));
    config.Load(node);

    RunTest(config, removeTables, createTables);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

using namespace NYT;

int main(int argc, char* argv[])
{
    try {
        GuardedMain(argc, argv);
    } catch (const std::exception& ex) {
        Cout << ToString(TError(ex)) << Endl;
    }
}
