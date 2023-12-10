#include "transaction_manager.h"

#include <yt/yt/server/lib/transaction_supervisor/proto/transaction_supervisor.pb.h>

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NTransactionSupervisor {

using namespace NApi;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TTransactionPrepareOptions* protoOptions,
    const TTransactionPrepareOptions& options)
{
    protoOptions->set_persistent(options.Persistent);
    protoOptions->set_late_prepare(options.LatePrepare);
    protoOptions->set_prepare_timestamp(options.PrepareTimestamp);
    protoOptions->set_prepare_timestamp_cluster_tag(
        ToProto<ui32>(options.PrepareTimestampClusterTag));
    ToProto(
        protoOptions->mutable_prerequisite_transaction_ids(),
        options.PrerequisiteTransactionIds);
}

void FromProto(
    TTransactionPrepareOptions* options,
    const NProto::TTransactionPrepareOptions& protoOptions)
{
    options->Persistent = protoOptions.persistent();
    options->LatePrepare = protoOptions.late_prepare();
    options->PrepareTimestamp = protoOptions.prepare_timestamp();
    options->PrepareTimestampClusterTag = FromProto<TClusterTag>(
        protoOptions.prepare_timestamp_cluster_tag());
    FromProto(
        &options->PrerequisiteTransactionIds,
        protoOptions.prerequisite_transaction_ids());
}

void ToProto(
    NProto::TTransactionCommitOptions* protoOptions,
    const TTransactionCommitOptions& options)
{
    protoOptions->set_commit_timestamp(options.CommitTimestamp);
    protoOptions->set_commit_timestamp_cluster_tag(
        ToProto<ui32>(options.CommitTimestampClusterTag));
}

void FromProto(
    TTransactionCommitOptions* options,
    const NProto::TTransactionCommitOptions& protoOptions)
{
    options->CommitTimestamp = protoOptions.commit_timestamp();
    options->CommitTimestampClusterTag = FromProto<TClusterTag>(
        protoOptions.commit_timestamp_cluster_tag());
}

void ToProto(
    NProto::TTransactionAbortOptions* protoOptions,
    const TTransactionAbortOptions& options)
{
    protoOptions->set_force(options.Force);
}

void FromProto(
    TTransactionAbortOptions* options,
    const NProto::TTransactionAbortOptions& protoOptions)
{
    options->Force = protoOptions.force();
}

////////////////////////////////////////////////////////////////////////////////

void TTransactionCommitOptions::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, CommitTimestamp);
    Persist(context, CommitTimestampClusterTag);
}

void TTransactionAbortOptions::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Force);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
