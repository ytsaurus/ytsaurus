#pragma once

#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NYdbTest {

////////////////////////////////////////////////////////////////////////////////

struct TDeliveryProbeEnvelope
    : public NYTree::TYsonStruct
{
    std::string RunId;
    std::string Lane;
    std::string ProducerId;
    ui64 Sequence = 0;
    ui64 ProbeTime = 0;
    ui64 BucketStart = 0;
    std::string Data;

    REGISTER_YSON_STRUCT(TDeliveryProbeEnvelope);

    static void Register(TRegistrar registrar);
};

using TDeliveryProbeEnvelopePtr = TIntrusivePtr<TDeliveryProbeEnvelope>;

struct TDeliveryProbeGeneratorState
    : public NYTree::TYsonStruct
{
    ui64 NextSequence = 0;
    ui64 LastProbeTime = 0;

    REGISTER_YSON_STRUCT(TDeliveryProbeGeneratorState);

    static void Register(TRegistrar registrar);
};

using TDeliveryProbeGeneratorStatePtr = TIntrusivePtr<TDeliveryProbeGeneratorState>;

struct TDeliveryProbeGeneratorParameters
    : public NYTree::TYsonStruct
{
    std::string RunId;
    std::string Lane;
    TDuration WindowSize;
    TStreamId GeneratedStreamId = "generated";
    TStreamId ExpectedStreamId = "expected";

    REGISTER_YSON_STRUCT(TDeliveryProbeGeneratorParameters);

    static void Register(TRegistrar registrar);
};

using TDeliveryProbeGeneratorParametersPtr = TIntrusivePtr<TDeliveryProbeGeneratorParameters>;

struct TDeliveryProbeOutputReaderParameters
    : public NYTree::TYsonStruct
{
    std::string RunId;
    std::string Lane;
    TStreamId ObservedStreamId = "observed";

    REGISTER_YSON_STRUCT(TDeliveryProbeOutputReaderParameters);
    static void Register(TRegistrar registrar);
};

class TDeliveryProbeOutputReader
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override;
    void ProcessMessage(const TInputMessageConstPtr& message, const IOutputCollectorPtr& output, const IRuntimeContextPtr& context) override;

private:
    std::string RunId_;
    std::string Lane_;
    TStreamId ObservedStreamId_;
};

// Converts trusted finite reference fixtures to the generator's expected stream.
class TDeliveryProbeExpectedReader
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override;
    void ProcessMessage(const TInputMessageConstPtr& message, const IOutputCollectorPtr& output, const IRuntimeContextPtr& context) override;

private:
    TIntrusivePtr<TDeliveryProbeGeneratorParameters> Parameters_;
};

struct TDeliveryValidationWindow
    : public NYTree::TYsonStruct
{
    ui64 BucketStart = 0;
    std::optional<ui64> FirstSequence;
    std::optional<ui64> IssuedThrough;
    std::vector<ui64> ObservedSequences;

    REGISTER_YSON_STRUCT(TDeliveryValidationWindow);
    static void Register(TRegistrar registrar);
};

struct TDeliveryValidationProducer
    : public NYTree::TYsonStruct
{
    std::string ProducerId;
    std::optional<ui64> InitialSequence;
    ui64 ClosedThrough = 0;
    std::vector<TIntrusivePtr<TDeliveryValidationWindow>> Windows;

    REGISTER_YSON_STRUCT(TDeliveryValidationProducer);
    static void Register(TRegistrar registrar);
};

struct TDeliveryValidationState
    : public NYTree::TYsonStruct
{
    std::string FirstErrorKind;
    std::string FirstErrorDetail;
    ui64 CheckedProbeCount = 0;
    ui64 AllowedDuplicateCount = 0;
    std::vector<TIntrusivePtr<TDeliveryValidationProducer>> Producers;

    REGISTER_YSON_STRUCT(TDeliveryValidationState);
    static void Register(TRegistrar registrar);
};

struct TDeliveryProbeCombinedValidatorParameters
    : public NYTree::TYsonStruct
{
    std::string RunId;
    std::string Lane;
    bool ExactlyOnce = false;
    TDuration WindowSize;
    TStreamId ExpectedStreamId = "expected";
    TStreamId ObservedStreamId = "observed";
    TStreamId CloseTimerStreamId = "close";
    TStreamId StatusTimerStreamId = "status";

    REGISTER_YSON_STRUCT(TDeliveryProbeCombinedValidatorParameters);
    static void Register(TRegistrar registrar);
};

class TDeliveryProbeValidator
    : public IKeyedBatchProcessFunction
{
public:
    explicit TDeliveryProbeValidator(const TProcessFunctionContextPtr& functionContext);
    void Init(const IRuntimeInitContextPtr& initContext) override;
    void ProcessKey(const IInputContextPtr& input, const IOutputCollectorPtr& output, const IRuntimeContextPtr& context) override;

private:
    TIntrusivePtr<TDeliveryProbeCombinedValidatorParameters> Parameters_;
    TMutableStateKeyClient<TDeliveryValidationState> StateClient_;
    IStatusErrorStatePtr ErrorState_;
};

class TDeliveryProbeGenerator
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override;

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    std::string RunId_;
    std::string Lane_;
    TDuration WindowSize_;
    TStreamId GeneratedStreamId_;
    TStreamId ExpectedStreamId_;
    TMutableStateKeyClient<TDeliveryProbeGeneratorState> GeneratorStateClient_;
};

struct TExpectedProbeRange
    : public TYsonMessage
{
    std::string RunId;
    std::string Lane;
    std::string ProducerId;
    ui64 BucketStart = 0;
    ui64 FirstSequence = 0;
    ui64 IssuedThrough = 0;

    REGISTER_YSON_STRUCT(TExpectedProbeRange);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExpectedProbeRange);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYdbTest
