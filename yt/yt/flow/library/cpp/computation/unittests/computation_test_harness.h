#pragma once

#include <yt/yt/flow/library/cpp/computation/computation_base.h>

#include <yt/yt/client/table_client/schema.h>

#include <functional>
#include <span>

namespace NYT::NFlow::NComputationTesting {

////////////////////////////////////////////////////////////////////////////////

//! Group-by schema of the computations under test: a single required uint64 column "key".
NTableClient::TTableSchemaPtr GetTestKeySchema();

//! Builds an input message of |streamId| with an empty payload and all timestamps equal to 1.
TInputMessageConstPtr MakeTestInputMessage(const TStreamId& streamId, const TMessageId& messageId, TKey key);

//! A field value whose serialization throws.
struct TUnserializable
{ };

void Serialize(const TUnserializable& value, NYson::IYsonConsumer* consumer);
void Deserialize(TUnserializable& value, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

//! What a computation run by #RunComputation() reads and where it reports. The callbacks are optional
//! and run on the serialized invoker of the computation.
struct TComputationRunOptions
{
    TComputationSpecPtr Spec;
    //! Schema of every output stream of #Spec.
    NTableClient::TTableSchemaPtr OutputSchema = New<NTableClient::TTableSchema>();
    //! Schemas of the output streams that differ from #OutputSchema: two streams cannot share a schema.
    THashMap<TStreamId, NTableClient::TTableSchemaPtr> OutputSchemaOverrides;
    //! Returns the next batch of the input streams; the batches are empty if unset.
    std::function<std::vector<TInputMessageConstPtr>()> GetNextBatch;
    //! Called on every commit of a transaction.
    std::function<void()> OnCommit;
    std::function<void(std::span<const TMessageId> messageIds)> OnMarkPersisted;
    std::function<void(std::span<const TOutputMessageConstPtr> messages)> OnRegisterOutputMessages;
};

//! Creates the computation under test.
using TComputationFactory = std::function<IComputationPtr(TComputationContextPtr context, TDynamicComputationContextPtr dynamicContext)>;

////////////////////////////////////////////////////////////////////////////////

//! Runs the computation that |factory| creates for |options| on a dedicated action queue with mock
//! clients until it throws, and returns the error. The current timestamp of the watermark state is 1000,
//! so a timer with a smaller trigger timestamp fires in the epoch after its registration.
TError RunComputation(const TComputationRunOptions& options, const TComputationFactory& factory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NComputationTesting
