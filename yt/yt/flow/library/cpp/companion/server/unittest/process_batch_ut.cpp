#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/server/server.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/companion/companion_client_detail.h>
#include <yt/yt/flow/library/cpp/companion/companion_proxy.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/testing/common/network.h>

#include <util/generic/map.h>
#include <util/system/datetime.h>
#include <util/system/getpid.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Emits every input message converted to the output stream.
class TUnittestPassthroughFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        output->AddMessage(context->ConvertToOutputMessage(*message));
    }
};

//! Counts messages per key in the internal state |counter| and emits the count.
class TUnittestCountingFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitClient(Counter_, "counter");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        auto state = Counter_.GetState(message);
        *state += 1;

        auto builder = context->MakeOutputMessageBuilder(std::nullopt);
        builder.SetMessageId(TMessageId(Format("out-%v", message->MessageId)));
        builder.SetSystemTimestamp(message->SystemTimestamp);
        builder.SetAlignmentTimestamp(message->AlignmentTimestamp);
        builder.Payload().Set(static_cast<ui64>(*state), "key");
        output->AddMessage(builder.Finish());
    }

private:
    TMutableStateKeyClient<i64> Counter_;
};

//! Burns a few milliseconds of CPU; used to test the cpu_time_ns metric.
class TUnittestBusyFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& /*message*/,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        auto start = ThreadCPUTime();
        volatile ui64 sink = 0;
        // ~2ms of thread CPU (ThreadCPUTime is in microseconds).
        while (ThreadCPUTime() - start < 2000) {
            for (int i = 0; i < 1000; ++i) {
                sink += i;
            }
        }
    }
};

//! Sync functions cannot run out of process; used to test the rejection path.
class TUnittestSyncFunction
    : public IProcessFunction
    , public ISyncProcessFunction
{
public:
    void Sync(
        const IRetryableTransactionPtr& /*transaction*/,
        const IRuntimeContextPtr& /*context*/) override
    { }
};

// The passthrough function is registered through the typed pipeline API in the
// fixture; the extra spec-selectable functions keep using the macro.
YT_FLOW_DEFINE_PROCESS_FUNCTION(TUnittestCountingFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TUnittestBusyFunction);
YT_FLOW_DEFINE_PROCESS_FUNCTION(TUnittestSyncFunction);

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NYTree;

using NYT::FromProto;
using NYT::ToProto;

constexpr TStringBuf KeySchemaYson = R"([{name = "key"; type = "uint64"}])";

class TProcessBatchTest
    : public ::testing::Test
{
protected:
    ::NTesting::TPortHolder Port_;
    TCompanionServerPtr Server_;
    std::optional<NCompanion::TCompanionProxy> Proxy_;

    NTableClient::TTableSchemaPtr Schema_ = NTesting::DefaultTestKeySchema();
    //! A distinct schema object: TStreamSpecs requires unique schema pointers per stream.
    NTableClient::TTableSchemaPtr OutputSchema_ = New<NTableClient::TTableSchema>(Schema_->Columns());
    TStreamSpecsPtr StreamSpecs_;
    TJobId JobId_ = TJobId(TGuid::Create());

    void SetUp() override
    {
        Port_ = ::NTesting::GetFreePort();

        auto config = New<NCompanion::TCompanionExecutionConfig>();
        config->Port = Port_;

        TPipeline pipeline;
        pipeline.AddTransform<TUnittestPassthroughFunction>("my_computation");

        Server_ = New<TCompanionServer>(config, pipeline);
        Server_->Start();
        Proxy_.emplace(NCompanion::CreateCompanionProxy(
            Format("localhost:%v", static_cast<int>(Port_))));

        THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecMap;
        for (const auto& [streamId, specId, schema] : {
                std::tuple{TStreamId("input"), TStreamSpecId(1), Schema_},
                std::tuple{TStreamId("output"), TStreamSpecId(2), OutputSchema_}})
        {
            auto streamSpec = New<TStreamSpec>();
            streamSpec->Schema = schema;
            streamSpecMap[streamId][specId] = std::move(streamSpec);
        }
        StreamSpecs_ = New<TStreamSpecs>(streamSpecMap);
    }

    void TearDown() override
    {
        Server_->Stop();
    }

    std::string BuildSpecYson(const std::string& functionName)
    {
        return Format(R"({
                computation_class_name = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
                processing_function = %Qv;
                group_by_schema = %v;
                input_stream_ids = ["input"];
                output_stream_ids = ["output"];
                parameters = {internal_states = ["counter"]};
            })",
            functionName,
            KeySchemaYson);
    }

    void FillJobInfo(
        NProto::NCompanion::TJobInfo* jobInfo,
        const std::string& functionName)
    {
        jobInfo->set_spec(BuildSpecYson(functionName));
        jobInfo->set_dynamic_spec("{}");
        for (const auto& [streamId, specId, schema] : {
                std::tuple{TStreamId("input"), TStreamSpecId(1), Schema_},
                std::tuple{TStreamId("output"), TStreamSpecId(2), OutputSchema_}})
        {
            auto* stream = jobInfo->add_streams();
            stream->set_stream_id(ToProto<TProtobufString>(streamId));
            stream->set_stream_spec_id(specId.Underlying());
            stream->set_schema(ToProto(NYson::ConvertToYsonString(schema)));
        }
    }

    auto BuildRequest(const std::optional<std::string>& functionName)
    {
        auto req = Proxy_->ProcessBatch();
        ToProto(req->mutable_request_id(), TGuid::Create());
        ToProto(req->mutable_job_id(), JobId_);
        req->set_computation_id("my_computation");
        if (functionName) {
            FillJobInfo(req->mutable_job_info(), *functionName);
        }
        return req;
    }

    void AddMessage(auto& req, ui64 keyValue, const std::string& messageId)
    {
        auto message = NTesting::MakeTestMessage(
            TStreamId("input"),
            MakeKey(keyValue),
            Schema_,
            [&] (TMessageBuilder& builder) {
                builder.SetMessageId(TMessageId(messageId));
                builder.Payload().Set(keyValue, "key");
            });
        auto* protoMessage = req->add_messages();
        ToProto(protoMessage->mutable_message(), *message, StreamSpecs_);
        ToProto(protoMessage->mutable_key(), message->Key);
    }
};

TEST_F(TProcessBatchTest, JobNotFoundThenHealed)
{
    {
        auto rsp = BuildRequest(std::nullopt)->Invoke().BlockingGet().ValueOrThrow();
        EXPECT_EQ(rsp->status(), NProto::NCompanion::RS_JOB_NOT_FOUND);
    }

    {
        auto req = BuildRequest("NYT::NFlow::NCompanionServer::TUnittestPassthroughFunction");
        AddMessage(req, 7, "m1");
        auto rsp = req->Invoke().BlockingGet().ValueOrThrow();
        ASSERT_EQ(rsp->status(), NProto::NCompanion::RS_OK);

        ASSERT_EQ(rsp->data().output_size(), 1);
        const auto& group = rsp->data().output(0);
        ASSERT_EQ(group.parent_ids_size(), 1);
        EXPECT_EQ(group.parent_ids(0), "m1");
        ASSERT_EQ(group.messages_size(), 1);
        auto message = FromProto<TMessage>(group.messages(0), StreamSpecs_);
        EXPECT_EQ(message.StreamId, TStreamId("output"));
        EXPECT_EQ(GetColumnValue<ui64>(message, 0), ui64{7});
    }

    {
        // The job is now cached; no job info needed.
        auto req = BuildRequest(std::nullopt);
        AddMessage(req, 8, "m2");
        auto rsp = req->Invoke().BlockingGet().ValueOrThrow();
        EXPECT_EQ(rsp->status(), NProto::NCompanion::RS_OK);
        EXPECT_EQ(rsp->data().output_size(), 1);
    }
}

TEST_F(TProcessBatchTest, RemoveJob)
{
    {
        auto req = BuildRequest("NYT::NFlow::NCompanionServer::TUnittestPassthroughFunction");
        AddMessage(req, 1, "m1");
        auto rsp = req->Invoke().BlockingGet().ValueOrThrow();
        ASSERT_EQ(rsp->status(), NProto::NCompanion::RS_OK);
    }

    auto removeJob = [&] {
        auto req = Proxy_->RemoveJob();
        ToProto(req->mutable_request_id(), TGuid::Create());
        ToProto(req->mutable_job_id(), JobId_);
        return req->Invoke().BlockingGet().ValueOrThrow();
    };

    EXPECT_EQ(removeJob()->status(), NProto::NCompanion::RS_OK);

    {
        // The removed job is unknown until the worker heals it.
        auto req = BuildRequest(std::nullopt);
        AddMessage(req, 2, "m2");
        auto rsp = req->Invoke().BlockingGet().ValueOrThrow();
        EXPECT_EQ(rsp->status(), NProto::NCompanion::RS_JOB_NOT_FOUND);
    }

    // Removal is idempotent.
    EXPECT_EQ(removeJob()->status(), NProto::NCompanion::RS_OK);

    {
        // Job info heals the removed job: a registration processed after a
        // removal recreates the entry, and the worker's reconcile pass
        // reclaims it if its job is gone.
        auto req = BuildRequest("NYT::NFlow::NCompanionServer::TUnittestPassthroughFunction");
        AddMessage(req, 3, "m3");
        auto rsp = req->Invoke().BlockingGet().ValueOrThrow();
        EXPECT_EQ(rsp->status(), NProto::NCompanion::RS_OK);
    }
}

TEST_F(TProcessBatchTest, ListJobs)
{
    auto listJobs = [&] {
        auto req = Proxy_->ListJobs();
        ToProto(req->mutable_request_id(), TGuid::Create());
        return req->Invoke().BlockingGet().ValueOrThrow();
    };

    {
        auto rsp = listJobs();
        EXPECT_EQ(rsp->status(), NProto::NCompanion::RS_OK);
        EXPECT_EQ(rsp->job_ids_size(), 0);
        EXPECT_EQ(rsp->process_id(), static_cast<i64>(GetPID()));
    }

    {
        auto req = BuildRequest("NYT::NFlow::NCompanionServer::TUnittestPassthroughFunction");
        AddMessage(req, 1, "m1");
        auto rsp = req->Invoke().BlockingGet().ValueOrThrow();
        ASSERT_EQ(rsp->status(), NProto::NCompanion::RS_OK);
    }

    {
        auto rsp = listJobs();
        ASSERT_EQ(rsp->job_ids_size(), 1);
        EXPECT_EQ(FromProto<TJobId>(rsp->job_ids(0)), JobId_);
    }

    {
        auto req = Proxy_->RemoveJob();
        ToProto(req->mutable_request_id(), TGuid::Create());
        ToProto(req->mutable_job_id(), JobId_);
        req->Invoke().BlockingGet().ValueOrThrow();
    }

    EXPECT_EQ(listJobs()->job_ids_size(), 0);
}

TEST_F(TProcessBatchTest, ClientRemoveJob)
{
    {
        auto req = BuildRequest("NYT::NFlow::NCompanionServer::TUnittestPassthroughFunction");
        AddMessage(req, 1, "m1");
        auto rsp = req->Invoke().BlockingGet().ValueOrThrow();
        ASSERT_EQ(rsp->status(), NProto::NCompanion::RS_OK);
    }

    auto client = New<NCompanion::TCompanionClient>(
        Format("localhost:%v", static_cast<int>(Port_)),
        /*timeout*/ TDuration::Seconds(5),
        TExponentialBackoffOptions{},
        /*statusProfiler*/ nullptr);
    client->RemoveJob(JobId_).BlockingGet().ThrowOnError();

    {
        auto req = BuildRequest(std::nullopt);
        AddMessage(req, 2, "m2");
        auto rsp = req->Invoke().BlockingGet().ValueOrThrow();
        EXPECT_EQ(rsp->status(), NProto::NCompanion::RS_JOB_NOT_FOUND);
    }
}

TEST_F(TProcessBatchTest, StatefulCounterRoundTrip)
{
    std::string stateBytes;
    {
        auto req = BuildRequest("NYT::NFlow::NCompanionServer::TUnittestCountingFunction");
        AddMessage(req, 1, "m1");
        auto rsp = req->Invoke().BlockingGet().ValueOrThrow();
        ASSERT_EQ(rsp->status(), NProto::NCompanion::RS_OK);

        // First message: count becomes 1 and the state is echoed as modified.
        ASSERT_EQ(rsp->data().internal_states_size(), 1);
        const auto& state = rsp->data().internal_states(0);
        EXPECT_EQ(state.name(), "counter");
        ASSERT_EQ(state.stateitems_size(), 1);
        EXPECT_FALSE(state.stateitems(0).reset());
        stateBytes = state.stateitems(0).state();

        ASSERT_EQ(rsp->data().output_size(), 1);
        auto message = FromProto<TMessage>(rsp->data().output(0).messages(0), StreamSpecs_);
        EXPECT_EQ(GetColumnValue<ui64>(message, 0), ui64{1});
    }

    {
        // Echo the state back like the worker does; the count continues from it.
        auto req = BuildRequest(std::nullopt);
        AddMessage(req, 1, "m2");
        auto* protoState = req->add_internal_states();
        protoState->set_name("counter");
        auto* item = protoState->add_stateitems();
        ToProto(item->mutable_key(), MakeKey(ui64{1}));
        item->set_reset(false);
        item->set_state(stateBytes);

        auto rsp = req->Invoke().BlockingGet().ValueOrThrow();
        ASSERT_EQ(rsp->status(), NProto::NCompanion::RS_OK);
        auto message = FromProto<TMessage>(rsp->data().output(0).messages(0), StreamSpecs_);
        EXPECT_EQ(GetColumnValue<ui64>(message, 0), ui64{2});
    }
}

TEST_F(TProcessBatchTest, CpuTimeReported)
{
    auto req = BuildRequest("NYT::NFlow::NCompanionServer::TUnittestBusyFunction");
    AddMessage(req, 1, "m1");
    auto rsp = req->Invoke().BlockingGet().ValueOrThrow();
    ASSERT_EQ(rsp->status(), NProto::NCompanion::RS_OK);
    // The busy function burns ~2ms of CPU; the accountant must observe it.
    EXPECT_GT(rsp->metrics().cpu_time_ns(), ui64{0});
}

TEST_F(TProcessBatchTest, UnknownComputationFails)
{
    auto req = BuildRequest("NYT::NFlow::NCompanionServer::TUnittestPassthroughFunction");
    req->set_computation_id("unknown");
    // Errors fail the RPC itself (parity with Java/Python), so the worker
    // retries them and sees the error text.
    auto rspOrError = req->Invoke().BlockingGet();
    ASSERT_FALSE(rspOrError.IsOK());
    EXPECT_THAT(
        ToString(static_cast<const TError&>(rspOrError)),
        testing::HasSubstr("is not registered in this companion"));
}

TEST_F(TProcessBatchTest, SyncFunctionRejected)
{
    auto req = BuildRequest("NYT::NFlow::NCompanionServer::TUnittestSyncFunction");
    AddMessage(req, 1, "m1");
    auto rspOrError = req->Invoke().BlockingGet();
    ASSERT_FALSE(rspOrError.IsOK());
    EXPECT_THAT(
        ToString(static_cast<const TError&>(rspOrError)),
        testing::HasSubstr("sync process functions are not supported"));
}

TEST_F(TProcessBatchTest, SourceStreamOverride)
{
    // The override carries fresh spec ids for the same streams, as source batches do.
    auto overrideSchema = New<NTableClient::TTableSchema>(Schema_->Columns());
    auto overrideOutputSchema = New<NTableClient::TTableSchema>(Schema_->Columns());
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> overrideMap;
    for (const auto& [streamId, specId, schema] : {
            std::tuple{TStreamId("input"), TStreamSpecId(11), overrideSchema},
            std::tuple{TStreamId("output"), TStreamSpecId(12), overrideOutputSchema}})
    {
        auto streamSpec = New<TStreamSpec>();
        streamSpec->Schema = schema;
        overrideMap[streamId][specId] = std::move(streamSpec);
    }
    auto overrideSpecs = New<TStreamSpecs>(overrideMap);

    auto req = BuildRequest("NYT::NFlow::NCompanionServer::TUnittestPassthroughFunction");
    for (const auto& [streamId, specId, schema] : {
            std::tuple{TStreamId("input"), TStreamSpecId(11), overrideSchema},
            std::tuple{TStreamId("output"), TStreamSpecId(12), overrideOutputSchema}})
    {
        auto* stream = req->add_streams();
        stream->set_stream_id(ToProto<TProtobufString>(streamId));
        stream->set_stream_spec_id(specId.Underlying());
        stream->set_schema(ToProto(NYson::ConvertToYsonString(schema)));
    }

    auto message = NTesting::MakeTestMessage(
        TStreamId("input"),
        MakeKey(ui64{5}),
        overrideSchema,
        [&] (TMessageBuilder& builder) {
            builder.Payload().Set(ui64{5}, "key");
        });
    auto* protoMessage = req->add_messages();
    ToProto(protoMessage->mutable_message(), *message, overrideSpecs);
    ToProto(protoMessage->mutable_key(), message->Key);

    auto rsp = req->Invoke().BlockingGet().ValueOrThrow();
    ASSERT_EQ(rsp->status(), NProto::NCompanion::RS_OK);
    ASSERT_EQ(rsp->data().output_size(), 1);
    // The output message is encoded against the override specs.
    EXPECT_EQ(rsp->data().output(0).messages(0).stream_spec_id(), 12);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanionServer
