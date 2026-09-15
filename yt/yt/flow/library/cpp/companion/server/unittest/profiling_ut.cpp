#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/server/monitoring.h>
#include <yt/yt/flow/library/cpp/companion/server/server.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/companion/companion_proxy.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/profiling/producer.h>
#include <yt/yt/library/profiling/solomon/exporter.h>
#include <yt/yt/library/profiling/solomon/registry.h>

#include <library/cpp/json/yson/json2yson.h>

#include <library/cpp/monlib/encode/json/json.h>

#include <library/cpp/testing/common/network.h>

#include <util/generic/map.h>
#include <util/stream/str.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Counts processed messages through the init-context profiler.
class TProfilingUnittestFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        MessageCount_ = initContext->GetProfiler().Counter("/profiling_ut/message_count");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override
    {
        MessageCount_.Increment();
        output->AddMessage(context->ConvertToOutputMessage(*message));
    }

private:
    NProfiling::TCounter MessageCount_;
};

//! Passes messages through; the base of the functions probing the origin tag.
class TPassThroughFunction
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

//! Builds a global sensor, which the exporter serializes without instance tags.
class TGlobalSensorFunction
    : public TPassThroughFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        Counter_ = initContext->GetProfiler().WithGlobal().Counter("/profiling_ut/global_count");
        Counter_.Increment();
    }

private:
    NProfiling::TCounter Counter_;
};

//! Reports through a producer that names the origin tag at collection time.
class TOriginNamingProducer
    : public NProfiling::ISensorProducer
{
public:
    void CollectSensors(NProfiling::ISensorWriter* writer) override
    {
        NProfiling::TWithTagGuard guard(writer, CompanionProcessTag, "worker");
        writer->AddGauge("/produced_gauge", 1.0);
    }
};

class TProducingFunction
    : public TPassThroughFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->GetProfiler().AddProducer("/profiling_ut", Producer_);
    }

private:
    const NProfiling::ISensorProducerPtr Producer_ = New<TOriginNamingProducer>();
};

//! Reports an origin tag through a buffered producer.
class TBufferedProducingFunction
    : public TPassThroughFunction
{
public:
    static inline TWeakPtr<NProfiling::TSensorBuffer> LastBuffer;
    static inline std::vector<TWeakPtr<NProfiling::TSensorBuffer>> Buffers;
    static inline int ProducerCount = 1;

    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        Buffers.clear();
        for (int index = 0; index < ProducerCount; ++index) {
            auto producer = New<NProfiling::TBufferedProducer>();
            producer->Update([] (NProfiling::ISensorWriter* writer) {
                NProfiling::TWithTagGuard guard(writer, CompanionProcessTag, "worker");
                writer->AddGauge("/buffered_gauge", 1.0);
            });
            auto buffer = producer->GetBuffer();
            LastBuffer = MakeWeak(buffer);
            Buffers.push_back(LastBuffer);
            initContext->GetProfiler().AddProducer("/profiling_ut", producer);
            Producers_.push_back(std::move(producer));
        }
    }

private:
    std::vector<NProfiling::TBufferedProducerPtr> Producers_;
};

class TStateProfilingFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override
    {
        initContext->InitClient(Internal_, "declared");
        initContext->InitExternalStateClient(External_, "external");
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        ++*Internal_.GetState(message);

        auto external = External_.GetState(message->Key);
        TPayloadBuilder builder(external->Schema);
        builder.Set(ui64{43}, "key");
        external->Payload = builder.Finish();
    }

private:
    TMutableStateKeyClient<i64> Internal_;
    TMutableStateKeyClient<TSimpleExternalState> External_;
};

class TFailingFunction
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& /*message*/,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        THROW_ERROR_EXCEPTION("Expected profiling test failure");
    }
};

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NYTree;

using NYT::ToProto;

//! A hosted computation and the process function class its job spec selects.
struct TTestFunction
{
    TStringBuf ComputationId;
    TStringBuf ClassName;
};

constexpr TStringBuf ComputationId = "my_computation";
constexpr TTestFunction CountingFunction{
    ComputationId,
    "NYT::NFlow::NCompanionServer::TProfilingUnittestFunction"};
constexpr TTestFunction GlobalSensorFunction{
    "global_sensor",
    "NYT::NFlow::NCompanionServer::TGlobalSensorFunction"};
constexpr TTestFunction ProducingFunction{
    "producing",
    "NYT::NFlow::NCompanionServer::TProducingFunction"};
constexpr TTestFunction BufferedProducingFunction{
    "buffered_producing",
    "NYT::NFlow::NCompanionServer::TBufferedProducingFunction"};
constexpr TTestFunction StateProfilingFunction{
    "state_profiling",
    "NYT::NFlow::NCompanionServer::TStateProfilingFunction"};
constexpr TTestFunction FailingFunction{
    "failing",
    "NYT::NFlow::NCompanionServer::TFailingFunction"};
constexpr TStringBuf KeySchemaYson = R"([{name = "key"; type = "uint64"}])";
//! Fast exporter grid for tests.
constexpr auto TestGridStep = TDuration::Seconds(1);

class TProfilingTest
    : public ::testing::Test
{
protected:
    ::NTesting::TPortHolder Port_;
    ::NTesting::TPortHolder MonitoringPort_;
    NProfiling::TSolomonRegistryPtr Registry_ = New<NProfiling::TSolomonRegistry>();
    TCompanionServerPtr Server_;
    std::optional<NCompanion::TCompanionProxy> Proxy_;

    NTableClient::TTableSchemaPtr Schema_ = NTesting::DefaultTestKeySchema();
    //! A distinct schema object: TStreamSpecs requires unique schema pointers per stream.
    NTableClient::TTableSchemaPtr OutputSchema_ = New<NTableClient::TTableSchema>(Schema_->Columns());
    TStreamSpecsPtr StreamSpecs_;

    void SetUp() override
    {
        TBufferedProducingFunction::ProducerCount = 1;
        Port_ = ::NTesting::GetFreePort();
        MonitoringPort_ = ::NTesting::GetFreePort();

        auto config = New<NCompanion::TCompanionExecutionConfig>();
        config->Port = Port_;
        config->MonitoringPort = MonitoringPort_;
        config->PipelinePath = "//tmp/pipeline";
        config->ClusterUrl = "test-cluster";
        config->Monitoring->GridStep = TestGridStep;

        TPipeline pipeline;
        pipeline.AddTransform<TProfilingUnittestFunction>(TComputationId(CountingFunction.ComputationId));
        pipeline.AddTransform<TGlobalSensorFunction>(TComputationId(GlobalSensorFunction.ComputationId));
        pipeline.AddTransform<TProducingFunction>(TComputationId(ProducingFunction.ComputationId));
        pipeline.AddTransform<TBufferedProducingFunction>(TComputationId(BufferedProducingFunction.ComputationId));
        pipeline.AddTransform<TStateProfilingFunction>(TComputationId(StateProfilingFunction.ComputationId));
        pipeline.AddTransform<TFailingFunction>(TComputationId(FailingFunction.ComputationId));

        Server_ = New<TCompanionServer>(config, pipeline, Registry_);
        Server_->Start();
        // Collect synchronously so summaries belong to the batch under test.
        Server_->GetMonitoring()->GetSolomonExporter()->Stop();
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

    void FillJobInfo(NProto::NCompanion::TJobInfo* jobInfo, const TTestFunction& function)
    {
        jobInfo->set_spec(Format(R"({
                computation_class_name = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
                processing_function = %Qv;
                group_by_schema = %v;
                input_stream_ids = ["input"];
                output_stream_ids = ["output"];
                parameters = {internal_states = ["declared"]};
                external_state_managers = {external = {};};
                external_state_joiners = {joined = {};};
            })",
            function.ClassName,
            KeySchemaYson));
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

    //! Sends one message; |withJobInfo| builds the job and |stateNames| adds empty states.
    NProto::NCompanion::EResponseStatus ProcessBatch(
        TJobId jobId,
        bool withJobInfo,
        const std::vector<std::string>& stateNames = {})
    {
        return ProcessBatchOrError(CountingFunction, jobId, withJobInfo, stateNames).ValueOrThrow();
    }

    //! The same for the computation of |function|, keeping an RPC error for the caller.
    TErrorOr<NProto::NCompanion::EResponseStatus> ProcessBatchOrError(
        const TTestFunction& function,
        TJobId jobId,
        bool withJobInfo,
        const std::vector<std::string>& stateNames = {})
    {
        auto req = Proxy_->ProcessBatch();
        ToProto(req->mutable_request_id(), TGuid::Create());
        ToProto(req->mutable_job_id(), jobId);
        req->set_computation_id(std::string(function.ComputationId));
        if (withJobInfo) {
            FillJobInfo(req->mutable_job_info(), function);
        }
        for (const auto& stateName : stateNames) {
            auto* state = req->add_internal_states();
            state->set_name(stateName);
        }

        auto message = NTesting::MakeTestMessage(
            TStreamId("input"),
            MakeKey(ui64{42}),
            Schema_,
            [&] (TMessageBuilder& builder) {
                builder.SetMessageId(TMessageId("m-1"));
                builder.Payload().Set(ui64{42}, "key");
            });
        auto* protoMessage = req->add_messages();
        ToProto(protoMessage->mutable_message(), *message, StreamSpecs_);
        ToProto(protoMessage->mutable_key(), message->Key);

        auto responseOrError = req->Invoke().BlockingGet();
        if (!responseOrError.IsOK()) {
            return TError(responseOrError);
        }
        return responseOrError.Value()->status();
    }

    std::vector<INodePtr> ReadSensors()
    {
        Registry_->ProcessRegistrations();
        auto iteration = Registry_->GetNextIteration();
        Registry_->Collect();

        NProfiling::TReadOptions options;
        options.Times = {{{Registry_->IndexOf(iteration)}, TInstant::Zero()}};
        options.EnableSolomonAggregates = true;
        options.SummaryPolicy = NProfiling::ESummaryPolicy::Max;
        options.InstanceTags = {
            {std::string(CompanionProcessTag), std::string(CompanionProcessTagValue)},
            {"pipeline_path", "//tmp/pipeline"},
            {"pipeline_cluster", "test-cluster"},
        };

        TStringStream buffer;
        auto encoder = ::NMonitoring::BufferedEncoderJson(&buffer);
        encoder->OnStreamBegin();
        Registry_->ReadSensors(options, encoder.Get());
        encoder->OnStreamEnd();
        encoder->Close();

        auto yson = NYson::TYsonString(
            NJson2Yson::SerializeJsonValueAsYson(NJson::ReadJsonFastTree(buffer.Str())));
        return ConvertToNode(yson)->AsMap()->GetChildOrThrow("sensors")->AsList()->GetChildren();
    }

    std::vector<INodePtr> WaitForResponseCounts(
        TStringBuf computationId,
        const std::vector<std::string>& statuses)
    {
        auto deadline = TInstant::Now() + TDuration::Seconds(10);
        while (TInstant::Now() < deadline) {
            auto sensors = ReadSensors();
            bool ready = true;
            for (const auto& status : statuses) {
                auto counter = FindSensor(sensors, "yt.flow.companion.request.response.count", {{"computation_id", std::string(computationId)}, {"status", status}});
                ready &= counter && counter->AsMap()->GetChildValueOrThrow<i64>("value") == 1;
            }
            if (ready) {
                return sensors;
            }
            // Reply delivery can precede the handler's accounting scope guard.
            Sleep(TDuration::MilliSeconds(1));
        }
        THROW_ERROR_EXCEPTION("Timed out waiting for ProcessBatch outcome counters");
    }

    static std::string DumpSensorNames(const std::vector<INodePtr>& sensors)
    {
        std::vector<std::string> names;
        for (const auto& sensor : sensors) {
            names.push_back(sensor->AsMap()->GetChildOrThrow("labels")->AsMap()->GetChildValueOrThrow<std::string>("sensor"));
        }
        return Format("%v", names);
    }

    //! Selects the projection matching every supplied label.
    static INodePtr FindSensor(
        const std::vector<INodePtr>& sensors,
        TStringBuf name,
        const THashMap<std::string, std::string>& labelsToMatch = {})
    {
        for (const auto& sensor : sensors) {
            auto labels = sensor->AsMap()->GetChildOrThrow("labels")->AsMap();
            if (labels->GetChildValueOrThrow<std::string>("sensor") != name) {
                continue;
            }
            bool matches = true;
            for (const auto& [name, value] : labelsToMatch) {
                if (labels->FindChildValue<std::string>(name) != value) {
                    matches = false;
                    break;
                }
            }
            if (matches) {
                return sensor;
            }
        }
        return nullptr;
    }

    static std::optional<std::string> FindLabel(const INodePtr& sensor, const std::string& name)
    {
        return sensor->AsMap()->GetChildOrThrow("labels")->AsMap()->FindChildValue<std::string>(name);
    }
};

////////////////////////////////////////////////////////////////////////////////

// Preserve user metric names and tags out of process.
TEST_F(TProfilingTest, UserFunctionSensorsAreExported)
{
    EXPECT_EQ(ProcessBatch(TJobId(TGuid::Create()), /*withJobInfo*/ true), NProto::NCompanion::RS_OK);

    auto sensors = ReadSensors();
    auto sensor = FindSensor(
        sensors,
        "yt.flow.worker.computation.profiling_ut.message_count",
        {{"computation_id", std::string(ComputationId)}});
    ASSERT_TRUE(sensor) << "sensors: " << DumpSensorNames(sensors);
    // Out of process the hosting partition is unknown, so there is nothing to tag with.
    EXPECT_FALSE(FindLabel(sensor, "partition_id"));
    // The required origin tag separates merged worker and companion series.
    EXPECT_EQ(FindLabel(sensor, "flow_process"), std::optional<std::string>("companion"));
}

// Verify per-computation service metrics and response statuses.
TEST_F(TProfilingTest, CompanionCountersFollowTheBatches)
{
    auto jobId = TJobId(TGuid::Create());
    EXPECT_EQ(ProcessBatch(jobId, /*withJobInfo*/ true), NProto::NCompanion::RS_OK);
    // A job this process never learned about is answered in band, and the worker retries.
    EXPECT_EQ(
        ProcessBatch(TJobId(TGuid::Create()), /*withJobInfo*/ false),
        NProto::NCompanion::RS_JOB_NOT_FOUND);

    auto sensors = WaitForResponseCounts(ComputationId, {"RS_OK", "RS_JOB_NOT_FOUND"});
    for (auto name : {
            "yt.flow.companion.request.count",
            "yt.flow.companion.request.size.max",
            "yt.flow.companion.request.duration.max",
            "yt.flow.companion.job.recreation.count",
            "yt.flow.companion.job.count"})
    {
        auto sensor = FindSensor(sensors, name);
        EXPECT_TRUE(sensor) << "sensor " << name << " is missing among " << DumpSensorNames(sensors);
    }

    // One reply-count family, cut by the in-band status rather than a name per status.
    for (auto status : {"RS_OK", "RS_JOB_NOT_FOUND"}) {
        auto sensor = FindSensor(
            sensors,
            "yt.flow.companion.request.response.count",
            {{"status", status}, {"computation_id", std::string(ComputationId)}});
        ASSERT_TRUE(sensor) << "reply count for " << status << " is missing among " << DumpSensorNames(sensors);
        EXPECT_EQ(sensor->AsMap()->GetChildValueOrThrow<i64>("value"), 1);
    }

    // The Java companion's schema, which the existing companion dashboards select on.
    auto requestCount = FindSensor(
        sensors,
        "yt.flow.companion.request.count",
        {{"computation_id", std::string(ComputationId)}});
    ASSERT_TRUE(requestCount) << "sensors: " << DumpSensorNames(sensors);
    EXPECT_EQ(FindLabel(requestCount, "request_type"), std::optional<std::string>("process_batch"));
    EXPECT_EQ(requestCount->AsMap()->GetChildValueOrThrow<i64>("value"), 2);
}

TEST_F(TProfilingTest, FailedBatchIsCountedAsErrorResponse)
{
    auto response = ProcessBatchOrError(
        FailingFunction,
        TJobId(TGuid::Create()),
        /*withJobInfo*/ true);
    ASSERT_FALSE(response.IsOK());

    auto sensors = WaitForResponseCounts(FailingFunction.ComputationId, {"RS_ERROR"});
    auto errorCount = FindSensor(
        sensors,
        "yt.flow.companion.request.response.count",
        {{"status", "RS_ERROR"}, {"computation_id", std::string(FailingFunction.ComputationId)}});
    ASSERT_TRUE(errorCount) << "sensors: " << DumpSensorNames(sensors);
    EXPECT_EQ(errorCount->AsMap()->GetChildValueOrThrow<i64>("value"), 1);
}

// Unknown computation IDs must not create lifetime-scoped sensors.
TEST_F(TProfilingTest, UnknownComputationBuildsNoSensors)
{
    auto request = Proxy_->ProcessBatch();
    ToProto(request->mutable_request_id(), TGuid::Create());
    ToProto(request->mutable_job_id(), TJobId(TGuid::Create()));
    request->set_computation_id("no_such_computation");

    auto responseOrError = request->Invoke().BlockingGet();
    ASSERT_FALSE(responseOrError.IsOK());
    EXPECT_THAT(
        ToString(static_cast<const TError&>(responseOrError)),
        testing::HasSubstr("is not registered in this companion"));

    auto sensors = ReadSensors();
    EXPECT_FALSE(FindSensor(
        sensors,
        "yt.flow.companion.request.count",
        {{"computation_id", std::string("no_such_computation")}}));
}

// Undeclared state names must not create lifetime-scoped sensors.
TEST_F(TProfilingTest, UndeclaredStatesBuildNoSensors)
{
    EXPECT_EQ(
        ProcessBatch(TJobId(TGuid::Create()), /*withJobInfo*/ true, {"declared", "undeclared"}),
        NProto::NCompanion::RS_OK);

    auto sensors = ReadSensors();
    auto stateSize = [&] (const std::string& stateName) {
        return FindSensor(
            sensors,
            "yt.flow.companion.state.size.max",
            {{"state_name", stateName}});
    };
    EXPECT_TRUE(stateSize("declared")) << "sensors: " << DumpSensorNames(sensors);
    EXPECT_FALSE(stateSize("undeclared"));
}

TEST_F(TProfilingTest, StateSizesCoverEveryWireSection)
{
    auto jobId = TJobId(TGuid::Create());
    auto req = Proxy_->ProcessBatch();
    ToProto(req->mutable_request_id(), TGuid::Create());
    ToProto(req->mutable_job_id(), jobId);
    req->set_computation_id(std::string(StateProfilingFunction.ComputationId));
    FillJobInfo(req->mutable_job_info(), StateProfilingFunction);

    req->add_internal_states()->set_name("declared");

    auto* externalState = req->add_external_states();
    externalState->set_name("external");
    externalState->set_schema(ToProto(NYson::ConvertToYsonString(Schema_)));
    auto* externalItem = externalState->add_stateitems();
    ToProto(externalItem->mutable_key(), MakeKey(ui64{42}));
    externalItem->set_reset(false);
    TPayloadBuilder externalPayloadBuilder(Schema_);
    externalPayloadBuilder.Set(ui64{42}, "key");
    externalItem->set_state(ToProto<TProtobufString>(externalPayloadBuilder.Finish()));

    auto* joinedState = req->add_joined_external_states();
    joinedState->set_name("joined");
    joinedState->set_schema(ToProto(NYson::ConvertToYsonString(Schema_)));

    auto message = NTesting::MakeTestMessage(
        TStreamId("input"),
        MakeKey(ui64{42}),
        Schema_,
        [&] (TMessageBuilder& builder) {
            builder.SetMessageId(TMessageId("m-state"));
            builder.Payload().Set(ui64{42}, "key");
        });
    auto* protoMessage = req->add_messages();
    ToProto(protoMessage->mutable_message(), *message, StreamSpecs_);
    ToProto(protoMessage->mutable_key(), message->Key);

    auto rsp = req->Invoke().BlockingGet().ValueOrThrow();
    ASSERT_EQ(rsp->status(), NProto::NCompanion::RS_OK);

    THashMap<std::string, double> stateMetrics;
    for (const auto& sensor : ReadSensors()) {
        if (FindLabel(sensor, "sensor") != std::optional<std::string>("yt.flow.companion.state.size.max") ||
            FindLabel(sensor, "computation_id") !=
                std::optional<std::string>(std::string(StateProfilingFunction.ComputationId)))
        {
            continue;
        }
        auto direction = FindLabel(sensor, "direction");
        auto stateType = FindLabel(sensor, "state_type");
        auto stateName = FindLabel(sensor, "state_name");
        if (direction && stateType && stateName) {
            stateMetrics[Format("%v/%v/%v", *direction, *stateType, *stateName)] =
                sensor->AsMap()->GetChildValueOrThrow<double>("value");
        }
    }

    ASSERT_EQ(rsp->data().internal_states_size(), 1);
    ASSERT_EQ(rsp->data().external_states_size(), 1);
    const THashMap<std::string, ui64> expectedSizes = {
        {"request/internal/declared", req->internal_states(0).ByteSizeLong()},
        {"request/external/external", req->external_states(0).ByteSizeLong()},
        {"request/joined_external/joined", req->joined_external_states(0).ByteSizeLong()},
        {"response/internal/declared", rsp->data().internal_states(0).ByteSizeLong()},
        {"response/external/external", rsp->data().external_states(0).ByteSizeLong()},
    };
    for (const auto& [labels, size] : expectedSizes) {
        ASSERT_TRUE(stateMetrics.contains(labels)) << labels << " is missing from " << Format("%v", stateMetrics);
        EXPECT_GT(size, 0u);
        EXPECT_EQ(stateMetrics.at(labels), static_cast<double>(size));
    }
}

// Global sensors must retain the origin tag.
TEST_F(TProfilingTest, GlobalSensorKeepsTheOriginTag)
{
    EXPECT_EQ(
        ProcessBatchOrError(GlobalSensorFunction, TJobId(TGuid::Create()), /*withJobInfo*/ true)
            .ValueOrThrow(),
        NProto::NCompanion::RS_OK);

    auto sensors = ReadSensors();
    auto sensor = FindSensor(
        sensors,
        "yt.flow.worker.computation.profiling_ut.global_count",
        {{"computation_id", std::string(GlobalSensorFunction.ComputationId)}});
    ASSERT_TRUE(sensor) << "sensors: " << DumpSensorNames(sensors);
    EXPECT_EQ(
        FindLabel(sensor, std::string(CompanionProcessTag)),
        std::optional<std::string>(CompanionProcessTagValue));
}

// Producer-emitted origin tags are replaced by the reserved value.
TEST_F(TProfilingTest, ProducerCannotOverrideTheOriginTag)
{
    EXPECT_EQ(
        ProcessBatchOrError(ProducingFunction, TJobId(TGuid::Create()), /*withJobInfo*/ true)
            .ValueOrThrow(),
        NProto::NCompanion::RS_OK);

    auto sensors = ReadSensors();
    auto sensor = FindSensor(
        sensors,
        "yt.flow.worker.computation.profiling_ut.produced_gauge",
        {{"computation_id", std::string(ProducingFunction.ComputationId)}});
    ASSERT_TRUE(sensor) << "sensors: " << DumpSensorNames(sensors);
    EXPECT_EQ(
        FindLabel(sensor, std::string(CompanionProcessTag)),
        std::optional<std::string>(CompanionProcessTagValue));
}

// Buffered producers must be filtered through #GetBuffer().
TEST_F(TProfilingTest, BufferedProducerIsCollected)
{
    EXPECT_EQ(
        ProcessBatchOrError(BufferedProducingFunction, TJobId(TGuid::Create()), /*withJobInfo*/ true)
            .ValueOrThrow(),
        NProto::NCompanion::RS_OK);

    auto sensors = ReadSensors();
    auto sensor = FindSensor(
        sensors,
        "yt.flow.worker.computation.profiling_ut.buffered_gauge",
        {{"computation_id", std::string(BufferedProducingFunction.ComputationId)}});
    ASSERT_TRUE(sensor) << "sensors: " << DumpSensorNames(sensors);
    EXPECT_EQ(
        FindLabel(sensor, std::string(CompanionProcessTag)),
        std::optional<std::string>(CompanionProcessTagValue));
}

TEST_F(TProfilingTest, BufferedProducerReleasesCachedBuffersAfterJobRemoval)
{
    auto jobId = TJobId(TGuid::Create());
    EXPECT_EQ(
        ProcessBatchOrError(BufferedProducingFunction, jobId, /*withJobInfo*/ true).ValueOrThrow(),
        NProto::NCompanion::RS_OK);

    auto buffer = TBufferedProducingFunction::LastBuffer;
    WaitForResponseCounts(BufferedProducingFunction.ComputationId, {"RS_OK"});
    ASSERT_FALSE(buffer.IsExpired());

    auto req = Proxy_->RemoveJob();
    ToProto(req->mutable_request_id(), TGuid::Create());
    ToProto(req->mutable_job_id(), jobId);
    EXPECT_EQ(
        req->Invoke().BlockingGet().ValueOrThrow()->status(),
        NProto::NCompanion::RS_OK);

    ReadSensors();
    EXPECT_TRUE(buffer.IsExpired());
}

TEST_F(TProfilingTest, ManyBufferedProducersAreReleasedAfterJobRemoval)
{
    TBufferedProducingFunction::ProducerCount = 1024;
    Registry_->SetProducerCollectionBatchSize(16);
    auto jobId = TJobId(TGuid::Create());
    ASSERT_EQ(
        ProcessBatchOrError(BufferedProducingFunction, jobId, /*withJobInfo*/ true).ValueOrThrow(),
        NProto::NCompanion::RS_OK);
    auto buffers = TBufferedProducingFunction::Buffers;
    ASSERT_EQ(std::ssize(buffers), TBufferedProducingFunction::ProducerCount);
    WaitForResponseCounts(BufferedProducingFunction.ComputationId, {"RS_OK"});
    for (const auto& buffer : buffers) {
        ASSERT_FALSE(buffer.IsExpired());
    }

    auto request = Proxy_->RemoveJob();
    ToProto(request->mutable_request_id(), TGuid::Create());
    ToProto(request->mutable_job_id(), jobId);
    ASSERT_EQ(request->Invoke().BlockingGet().ValueOrThrow()->status(), NProto::NCompanion::RS_OK);
    ReadSensors();
    for (const auto& buffer : buffers) {
        EXPECT_TRUE(buffer.IsExpired());
    }
    ReadSensors();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanionServer
