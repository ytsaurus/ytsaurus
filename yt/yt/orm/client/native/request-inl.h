#ifndef REQUEST_INL_H_
#error "Direct inclusion of this file is not allowed, include request.h"
// For the sake of sane code completion.
#include "request.h"
#endif

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

template <typename TPayloadProto>
void ToProto(
    TPayloadProto* protoPayload,
    const TObjectIdentity& objectIdentity)
{
    Visit(objectIdentity,
        [&] (const std::string& serializedKey) {
            NYson::TYsonString yson = NYTree::BuildYsonStringFluently()
                .BeginMap()
                    .Item("key").Value(serializedKey)
                .EndMap();
            protoPayload->set_yson(yson.ToString());
        },
        [&] (const TPayload& meta) {
            ToProto(protoPayload, meta);
        });
}

template <typename TMutatingTransactionOptionsProto>
void ToProto(TMutatingTransactionOptionsProto* protoOptions, const TMutatingTransactionOptions& options)
{
    protoOptions->set_skip_watch_log(options.SkipWatchLog);
    protoOptions->set_skip_history(options.SkipHistory);
    protoOptions->set_skip_revision_bump(options.SkipRevisionBump);
    if (options.AllowRemovalWithNonEmptyReference) {
        protoOptions->set_allow_removal_with_non_empty_reference(*options.AllowRemovalWithNonEmptyReference);
    }
    if (!options.TransactionContext.Items.empty()) {
        ToProto(
            protoOptions->mutable_transaction_context(),
            options.TransactionContext);
    }
}

template <class TUpdatesProto>
void FillAttributeTimestampPrerequisites(
    TUpdatesProto* updatesProto,
    std::vector<TAttributeTimestampPrerequisite> attributeTimestampPrerequisites)
{
    updatesProto->mutable_attribute_timestamp_prerequisites()->Reserve(attributeTimestampPrerequisites.size());
    for (auto& prerequisite : attributeTimestampPrerequisites) {
        auto* protoPrerequisite = updatesProto->add_attribute_timestamp_prerequisites();
        protoPrerequisite->set_path(std::move(prerequisite.Path));
        protoPrerequisite->set_timestamp(prerequisite.Timestamp);
    }
}

template <class TUpdatesProto>
void ToProto(
    TUpdatesProto* updatesProto,
    std::vector<NNative::TUpdate> updates)
{
    for (auto& update : updates) {
        NYT::Visit(update,
            [&] (NNative::TSetUpdate& setUpdate)
            {
                auto* protoSetUpdate = updatesProto->add_set_updates();
                protoSetUpdate->set_path(std::move(setUpdate.Path));
                ToProto(protoSetUpdate->mutable_value_payload(), setUpdate.Payload);
                protoSetUpdate->set_recursive(setUpdate.Recursive);
                if (setUpdate.SharedWrite) {
                    protoSetUpdate->set_shared_write(*setUpdate.SharedWrite);
                }
                protoSetUpdate->set_aggregate_mode(
                    NClient::NProto::EAggregateMode(static_cast<int>(setUpdate.AggregateMode)));
            },
            [&] (NNative::TSetRootUpdate& setRootUpdate)
            {
                auto* protoSetRootUpdate = updatesProto->add_set_root_updates();
                protoSetRootUpdate->mutable_paths()->Reserve(setRootUpdate.Paths.size());
                for (auto& path : setRootUpdate.Paths) {
                    protoSetRootUpdate->add_paths(std::move(path));
                }
                ToProto(protoSetRootUpdate->mutable_value_payload(), setRootUpdate.Payload);
                protoSetRootUpdate->set_recursive(setRootUpdate.Recursive);
                if (setRootUpdate.SharedWrite) {
                    protoSetRootUpdate->set_shared_write(*setRootUpdate.SharedWrite);
                }
                protoSetRootUpdate->set_aggregate_mode(
                    NClient::NProto::EAggregateMode(static_cast<int>(setRootUpdate.AggregateMode)));
            },
            [&] (NNative::TRemoveUpdate& removeUpdate)
            {
                auto* protoRemoveUpdate = updatesProto->add_remove_updates();
                protoRemoveUpdate->set_path(std::move(removeUpdate.Path));
                protoRemoveUpdate->set_force(removeUpdate.Force);
            },
            [&] (NNative::TLockUpdate& lockUpdate)
            {
                auto* protoLockUpdate = updatesProto->add_lock_updates();
                protoLockUpdate->set_path(std::move(lockUpdate.Path));
                protoLockUpdate->set_lock_type(
                    NOrm::NClient::NProto::ELockType(static_cast<int>(lockUpdate.LockType)));
            },
            [&] (NNative::TMethodCall& methodCall)
            {
                auto* protoMethodCall = updatesProto->add_method_calls();
                protoMethodCall->set_path(std::move(methodCall.Path));
                ToProto(protoMethodCall->mutable_value_payload(), methodCall.Payload);
            });
        }
}

template <class TProtoRequest, class TCommonOptions>
void FillCommonOptions(TProtoRequest protoRequest, const TCommonOptions& options)
{
    protoRequest->mutable_common_options()->set_fetch_performance_statistics(options.FetchPerformanceStatistics);
    if (options.AllowFullScan) {
        protoRequest->mutable_common_options()->set_allow_full_scan(*options.AllowFullScan);
    }
}

template <class EObjectType, class TUpdateRequest>
void FillUpdateObjectRequestAttributes(
    TObjectIdentity objectIdentity,
    NObjects::TObjectTypeValue objectType,
    std::vector<TUpdate> updates,
    std::vector<NNative::TAttributeTimestampPrerequisite> attributeTimestampPrerequisites,
    TUpdateRequest* req)
{
    YT_VERIFY(req);

    ToProto(req->mutable_meta(), std::move(objectIdentity));
    req->set_object_type(static_cast<EObjectType>(objectType));

    NClient::NNative::ToProto(req, std::move(updates));
    FillAttributeTimestampPrerequisites(req, std::move(attributeTimestampPrerequisites));
}

template <class TGetRequest>
void FillGetObjectRequestOptions(
    const NNative::TGetObjectOptions& options,
    TGetRequest* req)
{
    req->set_timestamp(options.Timestamp);
    req->set_format(static_cast<NOrm::NClient::NProto::EPayloadFormat>(options.Format));
    ToProto(req->mutable_timestamp_by_transaction_id(), options.TimestampByTransactionId);
    ToProto(req->mutable_transaction_id(), options.TransactionId);

    auto* protoOptions = req->mutable_options();
    if (options.IgnoreNonexistent && options.SkipNonexistent) {
        THROW_ERROR_EXCEPTION("Options `IgnoreNonexistent` and `SkipNonexistent` must not be set at the same time");
    } else if (options.IgnoreNonexistent) {
        protoOptions->set_ignore_nonexistent(options.IgnoreNonexistent);
    } else if (options.SkipNonexistent) {
        protoOptions->set_skip_nonexistent(options.SkipNonexistent);
    }
    protoOptions->set_fetch_values(options.FetchValues);
    protoOptions->set_fetch_timestamps(options.FetchTimestamps);
    protoOptions->set_fetch_root_object(options.FetchRootObject);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
