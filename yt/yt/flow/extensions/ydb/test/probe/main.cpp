#include <yt/yt/library/arcadia_future_interop/interop.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/convert.h>

#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/topic/client.h>
#include <ydb-cpp-sdk/client/topic/deferred_publications.h>
#include <ydb-cpp-sdk/client/types/credentials/credentials.h>

#include <library/cpp/json/json_writer.h>

#include <util/generic/guid.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/system/env.h>

#include <algorithm>
#include <mutex>
#include <thread>

namespace NYT::NFlow::NYdbProbe {

using namespace NConcurrency;
using namespace NYdb::NTopic;

////////////////////////////////////////////////////////////////////////////////

constexpr auto OperationTimeout = TDuration::Seconds(10);
constexpr auto MessageWaitTimeout = TDuration::Seconds(30);
constexpr auto InvisibilityWindow = TDuration::Seconds(3);
constexpr auto ConsumerName = "ytflow-fixture-probe";
constexpr auto WriterIdentity = "ytflow-fixture-probe";

////////////////////////////////////////////////////////////////////////////////

class TCollectingReader
{
public:
    TCollectingReader(TTopicClient& client, const std::string& topic)
        : Session_(client.CreateReadSession(TReadSessionSettings()
                .ConsumerName(ConsumerName)
                .AppendTopics(TTopicReadSettings(topic))
                .EventHandlers(TReadSessionSettings::TEventHandlers()
                        .SimpleDataHandlers([this] (TReadSessionEvent::TDataReceivedEvent& event) {
                            auto guard = std::lock_guard(Lock_);
                            for (const auto& message : event.GetMessages()) {
                                Messages_.push_back(message.GetData());
                            }
                        },
                            /*commitDataAfterProcessing*/ true))))
    { }

    ~TCollectingReader()
    {
        Session_->Close(OperationTimeout);
    }

    std::vector<std::string> CollectFor(TDuration window, size_t expectedCount)
    {
        auto deadline = TInstant::Now() + window;
        while (TInstant::Now() < deadline) {
            if (Snapshot().size() >= expectedCount) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return Snapshot();
    }

private:
    const std::shared_ptr<IReadSession> Session_;
    std::mutex Lock_;
    std::vector<std::string> Messages_;

    std::vector<std::string> Snapshot()
    {
        auto guard = std::lock_guard(Lock_);
        return Messages_;
    }
};

////////////////////////////////////////////////////////////////////////////////

void ThrowIfFailed(const NYdb::TStatus& status, TStringBuf operation)
{
    if (!status.IsSuccess()) {
        THROW_ERROR_EXCEPTION("%v probe failed", operation)
            .With("status", ToString(status.GetStatus()))
            .With("issues", status.GetIssues().ToString());
    }
}

std::string CreateProbeTopic(TTopicClient& client, TStringBuf mode)
{
    auto topic = Format("ytflow-fixture-probe-%v-%v", mode, CreateGuidAsString());
    auto settings = TCreateTopicSettings()
        .PartitioningSettings(1, 1)
        .SetSupportedCodecs({ECodec::RAW, ECodec::GZIP, ECodec::ZSTD});
    settings.BeginAddConsumer(ConsumerName).EndAddConsumer();
    settings.OperationTimeout(OperationTimeout);
    ThrowIfFailed(WaitFor(FromArcadiaFuture(client.CreateTopic(topic, settings))).ValueOrThrow(), "Topic creation");
    return topic;
}

void WriteMessage(TTopicClient& client, const std::string& topic, ECodec codec, TWriteMessage message)
{
    auto session = client.CreateSimpleBlockingWriteSession(TWriteSessionSettings()
            .Path(topic)
            .ProducerId(WriterIdentity)
            .MessageGroupId(WriterIdentity)
            .Codec(codec));
    THROW_ERROR_EXCEPTION_IF(!session->Write(std::move(message), nullptr, OperationTimeout),
        "Topic write probe was not accepted");
    THROW_ERROR_EXCEPTION_IF(!session->Close(OperationTimeout),
        "Topic write probe was not acknowledged");
}

void ExpectMessages(const std::vector<std::string>& actual, const std::vector<std::string>& expected, TStringBuf stage)
{
    if (actual != expected) {
        THROW_ERROR_EXCEPTION("Unexpected messages %v", stage)
            .With("expected", expected)
            .With("actual", actual);
    }
}

NJson::TJsonValue ProbeOrdinary(TTopicClient& client)
{
    auto topic = CreateProbeTopic(client, "ordinary");
    std::vector<std::pair<ECodec, std::string>> payloads = {
        {ECodec::RAW, "raw-payload"},
        {ECodec::GZIP, "gzip-payload"},
        {ECodec::ZSTD, "zstd-payload"},
    };
    std::vector<std::string> expected;
    for (const auto& [codec, payload] : payloads) {
        WriteMessage(client, topic, codec, TWriteMessage(payload));
        expected.push_back(payload);
    }
    TCollectingReader reader(client, topic);
    ExpectMessages(reader.CollectFor(MessageWaitTimeout, expected.size()), expected, "after ordinary write");

    NJson::TJsonValue result;
    result["create"] = "SUCCESS";
    result["write"] = NJson::TJsonArray();
    for (const auto& [codec, payload] : payloads) {
        result["write"].AppendValue(ToString(codec));
    }
    result["read"] = static_cast<int>(expected.size());
    return result;
}

TBeginPublicationResult BeginPublication(TDeferredPublishClient& client, const std::string& publicationId)
{
    return WaitFor(FromArcadiaFuture(client.BeginPublication(
        publicationId,
        TBeginPublicationSettings()
            .WriterIdentity(WriterIdentity)
            .OperationTimeout(OperationTimeout))))
        .ValueOrThrow();
}

NJson::TJsonValue ProbeDeferredEnabled(TTopicClient& topicClient, TDeferredPublishClient& client)
{
    auto begun = BeginPublication(client, "ytflow-fixture-probe");
    ThrowIfFailed(begun, "Deferred publication begin");
    auto listed = WaitFor(FromArcadiaFuture(client.ListPublications(
        TListPublicationsSettings().OperationTimeout(OperationTimeout))))
        .ValueOrThrow();
    ThrowIfFailed(listed, "Deferred publication list");
    const auto& publications = listed.GetPublications();
    THROW_ERROR_EXCEPTION_IF(
        std::none_of(publications.begin(), publications.end(), [&] (const auto& publication) {
            return publication.IntPublicationId == begun.GetIntPublicationId();
        }),
        "Deferred publication list probe does not contain the begun publication");
    auto canceled = WaitFor(FromArcadiaFuture(client.CancelPublication(
        begun.GetPublication(),
        TCancelPublicationSettings().OperationTimeout(OperationTimeout))))
        .ValueOrThrow();
    ThrowIfFailed(canceled, "Deferred publication cancel");

    auto topic = CreateProbeTopic(topicClient, "deferred");
    auto publishing = BeginPublication(client, "ytflow-fixture-probe-publish");
    ThrowIfFailed(publishing, "Deferred publication begin");
    const std::string payload = "deferred-payload";
    WriteMessage(topicClient, topic, ECodec::RAW, TWriteMessage(payload).DeferredPublication(publishing.GetPublication()));

    TCollectingReader reader(topicClient, topic);
    ExpectMessages(reader.CollectFor(InvisibilityWindow, 1), {}, "before publish");
    auto published = WaitFor(FromArcadiaFuture(client.Publish(
        publishing.GetPublication(),
        TPublishSettings().OperationTimeout(OperationTimeout))))
        .ValueOrThrow();
    ThrowIfFailed(published, "Deferred publication publish");
    ExpectMessages(reader.CollectFor(MessageWaitTimeout, 1), {payload}, "after publish");

    NJson::TJsonValue result;
    result["begin"] = "SUCCESS";
    result["list"] = "SUCCESS";
    result["cancel"] = "SUCCESS";
    result["publish"] = "SUCCESS";
    result["invisibleBeforePublish"] = true;
    result["visibleAfterPublish"] = true;
    return result;
}

NJson::TJsonValue ProbeDeferredDisabled(TDeferredPublishClient& client)
{
    auto begun = BeginPublication(client, "ytflow-fixture-probe-disabled");
    THROW_ERROR_EXCEPTION_IF(begun.IsSuccess(),
        "Deferred publication begin unexpectedly succeeded with the feature disabled");
    NJson::TJsonValue result;
    result["begin"] = ToString(begun.GetStatus());
    return result;
}

ui64 WriteDeferredMessage(
    TTopicClient& client,
    const std::string& topic,
    const TBeginPublicationResult& publication,
    const std::string& payload)
{
    auto session = client.CreateSimpleBlockingWriteSession(TWriteSessionSettings()
            .Path(topic)
            .ProducerId(WriterIdentity)
            .MessageGroupId(WriterIdentity)
            .Codec(ECodec::RAW));
    auto initSeqNo = session->GetInitSeqNo();
    auto message = TWriteMessage(payload);
    message.SeqNo(initSeqNo + 1).DeferredPublication(publication.GetPublication());
    THROW_ERROR_EXCEPTION_IF(
        !session->Write(std::move(message), nullptr, OperationTimeout),
        "Deferred write was not accepted");
    THROW_ERROR_EXCEPTION_IF(!session->Close(OperationTimeout),
        "Deferred write was not acknowledged");
    return initSeqNo;
}

NJson::TJsonValue ProbeCancelReusesSequence(TTopicClient& topicClient, TDeferredPublishClient& client)
{
    auto topic = CreateProbeTopic(topicClient, "cancel-reuse");

    auto cancelled = BeginPublication(client, "ytflow-fixture-probe-cancel-reuse-cancelled");
    ThrowIfFailed(cancelled, "Deferred publication begin");
    auto cancelledInitSeqNo = WriteDeferredMessage(topicClient, topic, cancelled, "cancelled");
    auto canceled = WaitFor(FromArcadiaFuture(client.CancelPublication(
        cancelled.GetPublication(),
        TCancelPublicationSettings().OperationTimeout(OperationTimeout))))
        .ValueOrThrow();
    ThrowIfFailed(canceled, "Deferred publication cancel");

    auto published = BeginPublication(client, "ytflow-fixture-probe-cancel-reuse-published");
    ThrowIfFailed(published, "Deferred publication begin");
    auto publishedInitSeqNo = WriteDeferredMessage(topicClient, topic, published, "published");
    auto publishResult = WaitFor(FromArcadiaFuture(client.Publish(
        published.GetPublication(),
        TPublishSettings().OperationTimeout(OperationTimeout))))
        .ValueOrThrow();
    ThrowIfFailed(publishResult, "Deferred publication publish");

    TCollectingReader reader(topicClient, topic);
    ExpectMessages(reader.CollectFor(MessageWaitTimeout, 1), {"published"}, "after cancel and publish");
    ExpectMessages(reader.CollectFor(InvisibilityWindow, 2), {"published"}, "after the invisibility window");

    NJson::TJsonValue result;
    result["cancelledInitSeqNo"] = cancelledInitSeqNo;
    result["publishedInitSeqNo"] = publishedInitSeqNo;
    result["visible"].AppendValue("published");
    return result;
}

NYdb::TDriverConfig MakeDriverConfig(const std::string& endpoint, const std::string& database)
{
    auto config = NYdb::TDriverConfig()
        .SetEndpoint(endpoint)
        .SetDatabase(database);
    if (auto user = GetEnv("YDB_USER"); !user.empty()) {
        config.SetCredentialsProviderFactory(NYdb::CreateLoginCredentialsProviderFactory({
            .User = user,
            .Password = GetEnv("YDB_PASSWORD"),
        }));
    }
    return config;
}

NJson::TJsonValue RunProbe(const std::string& endpoint, const std::string& database, TStringBuf mode)
{
    NYdb::TDriver driver(MakeDriverConfig(endpoint, database));
    NJson::TJsonValue result;
    try {
        TTopicClient topicClient(driver);
        TDeferredPublishClient publishClient(driver);
        if (mode == "ordinary") {
            result["operations"] = ProbeOrdinary(topicClient);
        } else if (mode == "deferred-enabled") {
            result["operations"] = ProbeDeferredEnabled(topicClient, publishClient);
        } else if (mode == "deferred-cancel-reuse") {
            result["operations"] = ProbeCancelReusesSequence(topicClient, publishClient);
        } else if (mode == "deferred-disabled") {
            result["operations"] = ProbeDeferredDisabled(publishClient);
        } else {
            THROW_ERROR_EXCEPTION("Unknown probe mode %Qv", mode);
        }
    } catch (...) {
        driver.Stop(true);
        throw;
    }
    driver.Stop(true);
    result["mode"] = mode;
    result["status"] = "SUCCESS";
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYdbProbe

int main(int argc, const char** argv)
{
    if (argc != 4) {
        Cerr << "Usage: " << argv[0] << " ENDPOINT DATABASE MODE" << Endl;
        return 2;
    }
    try {
        auto result = NYT::NFlow::NYdbProbe::RunProbe(argv[1], argv[2], argv[3]);
        NJson::WriteJson(&Cout, &result);
        Cout << Endl;
        return 0;
    } catch (const std::exception& ex) {
        NJson::TJsonValue failure;
        failure["mode"] = argv[3];
        failure["status"] = "FAILED";
        failure["error"] = ex.what();
        NJson::WriteJson(&Cout, &failure);
        Cout << Endl;
        return 1;
    }
}
