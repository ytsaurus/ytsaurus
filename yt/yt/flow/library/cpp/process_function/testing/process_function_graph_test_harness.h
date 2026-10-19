#pragma once

#include "test_state_environment.h"

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/process_function_graph.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/yson_message.h>

#include <string>
#include <vector>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

//! Runs a Transform or Swift map process function graph and commits managed test state between epochs.
//! Does not simulate job recovery or roll back a failed epoch. The graph is resolved like in
//! the runner; outputs pass production metadata and stream dependency checks.
class TProcessFunctionGraphTestHarness
{
public:
    struct TOutputMessage
    {
        TMessage Message;
        //! IDs of the computation input messages and timers the message derives from.
        std::vector<TMessageId> ParentIds;
    };

    struct TOutputTimer
    {
        TTimer Timer;
        std::string NodeId;
        //! Timer stream ID as declared on the node.
        TStreamId TimerStreamId;
        std::vector<TMessageId> ParentIds;
    };

    //! |computationYson| is a computation spec with `process_function_graph`, as in the pipeline spec.
    //! `computation_class_name` defaults to `TProcessFunctionComputation` and `group_by_schema` to |keySchema|;
    //! a set `group_by_schema` must equal |keySchema|, which must also be the key schema of |environment|.
    TProcessFunctionGraphTestHarness(
        TTestStateEnvironment& environment,
        TStringBuf computationYson,
        NTableClient::TTableSchemaPtr keySchema = DefaultTestKeySchema());

    TProcessFunctionGraphTestHarness(
        TTestStateEnvironment& environment,
        TComputationSpecPtr computation,
        NTableClient::TTableSchemaPtr keySchema = DefaultTestKeySchema());

    ~TProcessFunctionGraphTestHarness();

    //! Registers a stream carrying YSON message type |T|; every stream the nodes use must be registered.
    template <CYsonMessage T>
    TProcessFunctionGraphTestHarness& RegisterStream(const TStreamId& streamId);

    //! Registers a stream by its payload schema.
    TProcessFunctionGraphTestHarness& RegisterStream(const TStreamId& streamId, NTableClient::TTableSchemaPtr schema);
    TProcessFunctionGraphTestHarness& RegisterStream(const TStreamId& streamId, TStreamSpecPtr spec);

    //! Sets dynamic parameters of node |nodeId| from the next epoch on; null removes them.
    //! The next epoch validates them like the controller does.
    TProcessFunctionGraphTestHarness& SetNodeDynamicParameters(const std::string& nodeId, const NYTree::TYsonStructPtr& parameters);
    TProcessFunctionGraphTestHarness& SetCurrentTimestamp(TSystemTimestamp value);
    //! Sets the event watermark of a pipeline stream or timer stream by its global ID.
    TProcessFunctionGraphTestHarness& SetWatermark(const TStreamId& streamId, TSystemTimestamp value);

    //! Builds an input message of stream |streamId| with the registered schema.
    TInputMessageConstPtr MakeMessage(
        const TStreamId& streamId,
        const TKey& key,
        const TMessageBuilder::TInitFunction& init = {}) const;

    //! Runs one epoch: preloads state, processes the input and commits managed test state.
    //! The graph is resolved and initialized before the first epoch.
    void RunEpoch(std::vector<TInputMessageConstPtr> messages, std::vector<TInputTimerConstPtr> timers = {});

    //! Messages written to output streams of the computation during the most recent epoch.
    const std::vector<TOutputMessage>& GetMessages() const;
    //! Timers scheduled during the most recent epoch.
    const std::vector<TOutputTimer>& GetTimers() const;
    //! Input timers that fire the timers scheduled during the most recent epoch.
    std::vector<TInputTimerConstPtr> MakeFiringTimers() const;

    //! Reads state |name| of node |nodeId| for |key|.
    template <class TState>
    TState ReadNodeState(TStringBuf nodeId, TStringBuf name, const TKey& key);

    //! The computation spec after resolution; available after the first epoch.
    const TComputationSpecPtr& GetComputationSpec() const;

private:
    TTestStateEnvironment& Environment_;
    const NTableClient::TTableSchemaPtr KeySchema_;
    TComputationSpecPtr Computation_;
    THashMap<TStreamId, TStreamSpecPtr> Streams_;
    THashMap<TStreamId, std::pair<std::string, TStreamId>> TimerNodes_;
    const TProcessFunctionGraphDynamicParametersPtr DynamicParameters_;
    TSystemTimestamp CurrentTimestamp_ = TSystemTimestamp(1);
    THashMap<TStreamId, TSystemTimestamp> Watermarks_;
    i64 NextEpochSeqNo_ = 1;

    //! Set once the graph is initialized.
    IBatchProcessFunctionPtr Function_;
    bool InitializationStarted_ = false;
    std::vector<TOutputMessage> Messages_;
    std::vector<TOutputTimer> Timers_;

    void EnsureInitialized();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting

#define PROCESS_FUNCTION_GRAPH_TEST_HARNESS_INL_H_
#include "process_function_graph_test_harness-inl.h"
#undef PROCESS_FUNCTION_GRAPH_TEST_HARNESS_INL_H_
