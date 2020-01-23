#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TPod
    : public TObject
    , public NYT::TRefTracked<TPod>
{
public:
    static constexpr EObjectType Type = EObjectType::Pod;

    TPod(
        const TObjectId& id,
        const TObjectId& podSetId,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TPodSetAttribute = TParentAttribute<TPodSet>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPodSetAttribute, PodSet);

    class TStatus
    {
    public:
        explicit TStatus(TPod* pod);

        class TAgent
        {
        public:
            explicit TAgent(TPod* pod);

            static const TScalarAttributeSchema<TPod, EPodCurrentState> StateSchema;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<EPodCurrentState>, State);

            static const TScalarAttributeSchema<TPod, TString> IssPayloadSchema;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TString>, IssPayload);

            using TPodAgentPayload = NClient::NApi::NProto::TPodStatus_TAgent_TPodAgentPayload;
            static const TScalarAttributeSchema<TPod, TPodAgentPayload> PodAgentPayloadSchema;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TPodAgentPayload>, PodAgentPayload);

            using TEtc = NProto::TPodAgentStatusEtc;
            static const TScalarAttributeSchema<TPod, TEtc> EtcSchema;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
        };

        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TAgent, Agent);

        class TScheduling
        {
        public:
            explicit TScheduling(TPod* pod);

            using TEtc = NProto::TPodStatusSchedulingEtc;

            static const TScalarAttributeSchema<TPod, TObjectId> NodeIdSchema;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TObjectId>, NodeId);

            static const TScalarAttributeSchema<TPod, TEtc> EtcSchema;
            DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
        };

        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScheduling, Scheduling);

        static const TScalarAttributeSchema<TPod, ui64> GenerationNumberSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<ui64>, GenerationNumber);

        static const TScalarAttributeSchema<TPod, NTransactionClient::TTimestamp> AgentSpecTimestampSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<NTransactionClient::TTimestamp>, AgentSpecTimestamp);

        using TDynamicResourceStatus = std::vector<NClient::NApi::NProto::TPodDynamicResourceStatus>;
        static const TScalarAttributeSchema<TPod, TDynamicResourceStatus> DynamicResourcesSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TDynamicResourceStatus>, DynamicResources);

        using TEtc = NProto::TPodStatusEtc;
        static const TScalarAttributeSchema<TPod, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);

    class TSpec
    {
    public:
        explicit TSpec(TPod* pod);

        static const TManyToOneAttributeSchema<TPod, TNode> NodeSchema;
        using TNodeAttribute = TManyToOneAttribute<TPod, TNode>;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TNodeAttribute, Node);

        static const TScalarAttributeSchema<TPod, TString> IssPayloadSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TString>, IssPayload);

        using TPodAgentPayload = NClient::NApi::NProto::TPodSpec_TPodAgentPayload;
        static const TScalarAttributeSchema<TPod, TPodAgentPayload> PodAgentPayloadSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TPodAgentPayload>, PodAgentPayload);

        static const TScalarAttributeSchema<TPod, bool> EnableSchedulingSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<bool>, EnableScheduling);

        using TSecrets = THashMap<TString, NClient::NApi::NProto::TPodSpec_TSecret>;
        static const TScalarAttributeSchema<TPod, TSecrets> SecretsSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSecrets>, Secrets);

        static const TTimestampAttributeSchema UpdateTimestampSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TTimestampAttribute, UpdateTimestamp);

        using TDynamicResourceSpec = std::vector<NClient::NApi::NProto::TPodDynamicResourceSpec>;
        static const TScalarAttributeSchema<TPod, TDynamicResourceSpec> DynamicResourcesSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TDynamicResourceSpec>, DynamicResources);

        using TResourceCache = NClient::NApi::NProto::TPodSpec_TPodAgentResourceCache;
        static const TScalarAttributeSchema<TPod, TResourceCache> ResourceCacheSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TResourceCache>, ResourceCache);

        static const TManyToOneAttributeSchema<TPod, TAccount> AccountSchema;
        using TAccountAttribute = TManyToOneAttribute<TPod, TAccount>;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TAccountAttribute, Account);

        using TDynamicAttributes = NClient::NApi::NProto::TPodSpec_TDynamicAttributes;
        static const TScalarAttributeSchema<TPod, TDynamicAttributes> DynamicAttributesSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TDynamicAttributes>, DynamicAttributes);

        using TEtc = NProto::TPodSpecEtc;
        static const TScalarAttributeSchema<TPod, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
    };

    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    void UpdateEvictionStatus(
        EEvictionState state,
        EEvictionReason reason,
        const TString& message);

    TPodDisruptionBudget* GetDisruptionBudget();

    void RequestEviction(
        EEvictionReason reason,
        const TString& message,
        bool validateDisruptionBudget);

    void UpdateSchedulingStatus(
        ESchedulingState state,
        const TString& message,
        const TObjectId& nodeId = TObjectId());

    void ResetAgentStatus();

    void UpdateMaintenanceStatus(
        EPodMaintenanceState state,
        const TString& message,
        TGenericUpdate<NClient::NApi::NProto::TMaintenanceInfo> infoUpdate);
};

////////////////////////////////////////////////////////////////////////////////

bool IsUnsafePortoIssSpec(const NClient::NApi::NClusterApiProto::HostConfiguration& issSpec);
void ValidateIssPodSpecSafe(TPod* pod);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
