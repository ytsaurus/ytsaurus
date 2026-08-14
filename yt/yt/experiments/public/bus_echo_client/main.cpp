#include <yt/yt/library/program/program.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/message_handler.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/crypto/config.h>

#include <yt/yt/core/yson/writer.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT {

using namespace NBus;
using namespace NBus::NTcp;
using namespace NConcurrency;

static const auto Logger = NLogging::TLogger("BusEchoServer");

////////////////////////////////////////////////////////////////////////////////

class TBusEchoMessageHandler
    : public IMessageHandler
{
public:
    TBusEchoMessageHandler(int expectedCounter)
        : ExpectedCounter_(expectedCounter)
    { }

    virtual void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus,
        IDirectPlacementTransferPtr /*transfer*/,
        TPacketId /*packetId*/) noexcept override
    {
        const auto& peer = replyBus->GetEndpointDescription();
        auto id = Counter_++;

        YT_TLOG_INFO("Received message")
            .With("PartCount", message.Size())
            .With("Peer", peer)
            .With("MessageId", id);
        for (size_t i = 0; i < message.Size(); ++i) {
            YT_TLOG_INFO("Received message part")
                .With("PartIndex", i)
                .WithFormat("Part", "%Qv", message[i])
                .With("Size", message[i].Size())
                .With("MessageId", id);
        }

        if (id + 1 == ExpectedCounter_) {
            Promise_.Set();
        }
    }

    void Terminate(const TErrorOr<void>& errorOr)
    {
        Promise_.TrySet(errorOr);
    }

    TFuture<void> GetFuture()
    {
        return Promise_;
    }

private:
    const int ExpectedCounter_ = 0;
    std::atomic<int> Counter_ = 0;

    TPromise<void> Promise_ = NewPromise<void>();
};

class TBusEchoClient
    : public TProgram
{
public:
    TBusEchoClient()
    {
        Opts_.AddLongOption("address").StoreResult(&Address_).Required();
        Opts_.SetFreeArgsMin(0);
        Opts_.SetFreeArgsMax(100);
        Opts_.AddLongOption("flood").NoArgument().SetFlag(&Flood_);
        Opts_.AddLongOption("ca_file").StoreResult(&CAFile_);
        Opts_.AddLongOption("encryption_mode").StoreResult(&EncryptionMode_);
        Opts_.AddLongOption("verification_mode").StoreResult(&VerificationMode_);
    }

protected:
    void DoRun() override
    {
        DoSingleRun();

        if (!Flood_) {
            return;
        }

        NProfiling::TWallTimer timer;
        for (int index = 0; index < 1000; ++index) {
            timer.Restart();
            DoSingleRun();
            auto elapsed = timer.GetElapsedTime();
            if (elapsed > TDuration::MilliSeconds(200)) {
                Cout << Format("Attempt %v connected in %v", index, elapsed) << Endl;
            }
        }
    }

    void DoSingleRun()
    {
        auto config = New<TBusClientConfig>();
        config->Address = Address_;
        config->EnableAggressiveReconnect = true;

        if (!EncryptionMode_.empty()) {
            config->EncryptionMode = TEnumTraits<EEncryptionMode>::FromString(EncryptionMode_);
        }

        if (!VerificationMode_.empty()) {
            config->VerificationMode = TEnumTraits<EVerificationMode>::FromString(VerificationMode_);
        }

        if (!CAFile_.empty()) {
            config->CertificateAuthority = New<NCrypto::TPemBlobConfig>();
            config->CertificateAuthority->FileName = CAFile_;
        }

        YT_TLOG_INFO("Connecting echo client")
            .With("Address", Address_);

        auto client = CreateBusClient(config);
        auto handler = New<TBusEchoMessageHandler>(1);

        auto bus = client->CreateBus(handler);
        bus->SubscribeTerminated(BIND([handler] (const TErrorOr<void>& errorOr) {
            handler->Terminate(errorOr);
        }));

        const auto& parseResult = GetOptsParseResult();
        auto args = parseResult.GetFreeArgs();
        TSharedRefArrayBuilder arrayBuilder(args.size());

        std::vector<TSharedRef> parts;
        for (const auto& arg : args) {
            arrayBuilder.Add(TSharedRef::FromString(arg));
        }
        auto message = arrayBuilder.Finish();

        YT_TLOG_INFO("Getting ready future");

        auto readyFuture = bus->GetReadyFuture();
        auto res = WaitFor(readyFuture);
        if (!res.IsOK()) {
            YT_TLOG_INFO("Bus is not ready for use")
                .With("Error", res.GetMessage());
            return;
        }

        YT_TLOG_INFO("bus is ready for use");

        YT_TLOG_INFO("Sending message")
            .With("PartCount", message.Size());
        for (size_t i = 0; i < message.Size(); ++i) {
            YT_TLOG_INFO("Sending message part")
                .With("PartIndex", i)
                .WithFormat("Part", "%Qv", message[i])
                .With("Size", message[i].Size());
        }

        auto future = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
        future.Subscribe(BIND([] (const TError& error) {
            if (error.IsOK()) {
                YT_TLOG_INFO("Message was sent");
            } else {
                YT_TLOG_ERROR("Failed to send message")
                    .With(error);
            }
        }));

        WaitFor(handler->GetFuture()).ThrowOnError();
    }

private:
    std::string Address_;
    std::string CAFile_;
    std::string EncryptionMode_;
    std::string VerificationMode_;
    bool Flood_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::TBusEchoClient().Run(argc, argv);
}
