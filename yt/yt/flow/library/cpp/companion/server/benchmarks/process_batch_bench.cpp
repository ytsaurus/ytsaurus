#include <benchmark/benchmark.h>

#include <yt/yt/flow/library/cpp/companion/server/server.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/runtime_context.h>
#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/companion/client/companion_proxy.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/process_function/testing/entity_builders.h>

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/testing/common/network.h>

#include <util/generic/map.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

struct TBenchmarkParameters
    : public NYTree::TYsonStruct
{
    int StateCount = 0;
    int PayloadSize = 0;

    REGISTER_YSON_STRUCT(TBenchmarkParameters);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("state_count", &TThis::StateCount).Default(0);
        registrar.Parameter("payload_size", &TThis::PayloadSize).Default(0);
    }
};

//! Updates internal and external state without producing output messages.
class TBenchmarkFunction
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& context) override
    {
        auto parameters = context->GetParameters<TBenchmarkParameters>();
        Payload_.assign(parameters->PayloadSize, 'y');
        Internal_.resize(parameters->StateCount);
        External_.resize(parameters->StateCount);
        Joined_.resize(parameters->StateCount);
        for (int index = 0; index < parameters->StateCount; ++index) {
            auto name = Format("state_%v", index);
            context->InitClient(Internal_[index], name);
            context->InitExternalStateClient(External_[index], name);
            context->InitExternalStateClient(Joined_[index], Format("joined_%v", index));
        }
    }

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& /*output*/,
        const IRuntimeContextPtr& /*context*/) override
    {
        for (int index = 0; index < std::ssize(Internal_); ++index) {
            ++*Internal_[index].GetState(message);
            auto external = External_[index].GetState(message->Key);
            TPayloadBuilder builder(external->Schema);
            builder.Set(GetColumnValue<ui64>(*message, "key"), "key");
            builder.Set(Payload_, "payload");
            external->Payload = builder.Finish();
            auto joined = Joined_[index].GetState(message->Key);
            benchmark::DoNotOptimize(joined);
        }
    }

private:
    std::string Payload_;
    std::vector<TMutableStateKeyClient<i64>> Internal_;
    std::vector<TMutableStateKeyClient<TSimpleExternalState>> External_;
    std::vector<TJoinedStateKeyClient<TSimpleExternalState>> Joined_;
};

namespace {

using namespace NYTree;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

//! Keeps one exporter alive across benchmark calibration and repetitions.
class TBenchmarkServer
{
public:
    TBenchmarkServer()
    {
        auto config = New<NCompanion::TCompanionExecutionConfig>();
        config->Port = RpcPort_;
        config->ClusterUrl = "benchmark";
        config->PipelinePath = "//benchmark";
        // The merge-base companion ignores this port.
        config->MonitoringPort = MonitoringPort_;
        TPipeline pipeline;
        pipeline.AddTransform<TBenchmarkFunction, TBenchmarkParameters>("benchmark");
        Server_ = New<TCompanionServer>(config, pipeline);
        Server_->Start();
        Proxy_.emplace(NCompanion::CreateCompanionProxy(
            Format("localhost:%v", static_cast<int>(RpcPort_))));
    }

    ~TBenchmarkServer()
    {
        Server_->Stop();
    }

    NCompanion::TCompanionProxy& GetProxy()
    {
        return *Proxy_;
    }

private:
    ::NTesting::TPortHolder RpcPort_ = ::NTesting::GetFreePort();
    ::NTesting::TPortHolder MonitoringPort_ = ::NTesting::GetFreePort();
    TCompanionServerPtr Server_;
    std::optional<NCompanion::TCompanionProxy> Proxy_;
};

//! Measures one serialized client and server in the same process.
void BM_ProcessBatch(benchmark::State& state)
{
    int messageCount = static_cast<int>(state.range(0));
    int stateCount = static_cast<int>(state.range(1));
    int payloadSize = static_cast<int>(state.range(2));
    static TBenchmarkServer server;
    auto& proxy = server.GetProxy();

    auto schema = ConvertTo<NTableClient::TTableSchemaPtr>(NYson::TYsonStringBuf(
        "[{name=key;type=uint64};{name=payload;type=string}]"));
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamMap;
    streamMap[TStreamId("input")][TStreamSpecId(1)] = streamSpec;
    auto streamSpecs = New<TStreamSpecs>(streamMap);

    static const auto JobId = TJobId(TGuid::Create());
    auto put = proxy.PutJob();
    ToProto(put->mutable_request_id(), TGuid::Create());
    ToProto(put->mutable_job_id(), JobId);
    put->set_computation_id("benchmark");
    auto spec = New<TComputationSpec>();
    spec->ComputationClassName = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
    spec->ProcessingFunction = TypeName<TBenchmarkFunction>();
    spec->GroupBySchema = NTesting::DefaultTestKeySchema();
    spec->InputStreamIds = {TStreamId("input")};
    auto parameters = New<TBenchmarkParameters>();
    parameters->StateCount = stateCount;
    parameters->PayloadSize = payloadSize;
    spec->ProcessingFunctionParameters = ConvertTo<IMapNodePtr>(parameters);
    std::vector<std::string> internalNames;
    for (int index = 0; index < stateCount; ++index) {
        auto name = Format("state_%v", index);
        internalNames.push_back(name);
        spec->ExternalStateManagers[name] = New<TExternalStateManagerSpec>();
        spec->ExternalStateJoiners[Format("joined_%v", index)] = New<TExternalStateJoinerSpec>();
    }
    spec->Parameters = BuildYsonNodeFluently()
        .BeginMap()
        .Item("internal_states")
        .Value(internalNames)
        .EndMap()
        ->AsMap();
    put->mutable_job_info()->set_spec(ToProto(NYson::ConvertToYsonString(spec)));
    put->mutable_job_info()->set_dynamic_spec("{}");
    auto* stream = put->mutable_job_info()->add_streams();
    stream->set_stream_id("input");
    stream->set_stream_spec_id(1);
    stream->set_schema(ToProto(NYson::ConvertToYsonString(schema)));
    YT_VERIFY(put->Invoke().BlockingGet().ValueOrThrow()->status() == NProto::NCompanion::RS_OK);

    NProto::NCompanion::TReqProcessBatch data;
    ToProto(data.mutable_request_id(), TGuid::Create());
    ToProto(data.mutable_job_id(), JobId);
    data.set_computation_id("benchmark");
    std::string payload(payloadSize, 'x');
    for (int index = 0; index < messageCount; ++index) {
        auto message = NTesting::MakeTestMessage(TStreamId("input"), MakeKey(ui64(index)), schema, [&] (TMessageBuilder& builder) {
            builder.SetMessageId(TMessageId(Format("message_%v", index)));
            builder.Payload().Set(ui64(index), "key");
            builder.Payload().Set(payload, "payload");
        });
        auto* target = data.add_messages();
        ToProto(target->mutable_message(), *message, streamSpecs);
        ToProto(target->mutable_key(), message->Key);
    }
    for (int index = 0; index < stateCount; ++index) {
        auto name = Format("state_%v", index);
        data.add_internal_states()->set_name(name);
        auto* external = data.add_external_states();
        external->set_name(name);
        auto* joined = data.add_joined_external_states();
        joined->set_name(Format("joined_%v", index));
        for (auto* target : {external, joined}) {
            target->set_schema(ToProto(NYson::ConvertToYsonString(schema)));
            for (int key = 0; key < messageCount; ++key) {
                auto* item = target->add_stateitems();
                item->set_reset(false);
                ToProto(item->mutable_key(), MakeKey(ui64(key)));
                TPayloadBuilder builder(schema);
                builder.Set(ui64(key), "key");
                builder.Set(payload, "payload");
                item->set_state(ToProto<TProtobufString>(builder.Finish()));
            }
        }
    }

    auto invoke = [&] {
        auto request = proxy.ProcessBatch();
        request->CopyFrom(data);
        auto response = request->Invoke().BlockingGet().ValueOrThrow();
        YT_VERIFY(response->status() == NProto::NCompanion::RS_OK);
        return response;
    };
    for (int index = 0; index < 10; ++index) {
        invoke();
    }
    auto warmResponse = invoke();
    YT_VERIFY(warmResponse->data().internal_states_size() == stateCount);
    YT_VERIFY(warmResponse->data().external_states_size() == stateCount);
    for (auto _ : state) {
        auto response = invoke();
        benchmark::DoNotOptimize(response);
    }
    state.SetItemsProcessed(state.iterations() * messageCount);
    state.SetBytesProcessed(state.iterations() * data.ByteSizeLong());
    state.counters["request_bytes"] = data.ByteSizeLong();
    state.counters["response_bytes"] = warmResponse->ByteSizeLong();
}

BENCHMARK(BM_ProcessBatch)
    ->Args({0, 0, 0})
    ->Args({100, 0, 128})
    ->Args({128, 16, 512})
    ->MeasureProcessCPUTime()
    ->UseRealTime();

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanionServer
