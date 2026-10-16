#pragma once

#include <yt/yt/flow/library/cpp/common/process_function.h>

#include <yt/yt/flow/library/cpp/computation/simple_external_state_manager.h>

#include <yt/yt/flow/library/cpp/common/state_client.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow::NExample {

////////////////////////////////////////////////////////////////////////////////

//! A row of the static reference table: a key and a human-entered name that needs
//! normalizing before it can be used as a join attribute.
struct TReferenceRow
    : public TYsonMessage
{
    ui64 Key{};
    std::string Name;

    REGISTER_YSON_STRUCT(TReferenceRow);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! An event of the realtime stream, carrying only the key to enrich.
struct TEventRow
    : public TYsonMessage
{
    ui64 Key{};

    REGISTER_YSON_STRUCT(TEventRow);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Output of the join: the event key enriched with the normalized name.
struct TEnrichedRow
    : public TYsonMessage
{
    ui64 Key{};
    std::string Name;

    REGISTER_YSON_STRUCT(TEnrichedRow);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Streams the static reference table row by row into the "reference" stream. Registered with
//! YT_FLOW_DEFINE_PROCESS_FUNCTION and wired to a source computation in the spec.
class TReferenceReader
    : public IProcessFunction
{
public:
    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;
};

////////////////////////////////////////////////////////////////////////////////

//! Applies the per-row transform (trim + lowercase) and writes the normalized name into the
//! shared keyed state table under "/reference_state".
class TReferenceLoader
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override;

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    TMutableStateKeyClient<TSimpleExternalState> ReferenceStateClient_;
};

////////////////////////////////////////////////////////////////////////////////

//! Joins each realtime event against the shared reference state (read-only) and emits the
//! enriched row into the "enriched" stream.
class TEnricher
    : public IProcessFunction
{
public:
    void Init(const IRuntimeInitContextPtr& initContext) override;

    void ProcessMessage(
        const TInputMessageConstPtr& message,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) override;

private:
    TJoinedStateKeyClient<TSimpleExternalState> ReferenceStateJoiner_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NExample
