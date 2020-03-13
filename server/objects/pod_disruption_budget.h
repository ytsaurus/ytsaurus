#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TPodDisruptionBudget
    : public TObject
    , public NYT::TRefTracked<TPodDisruptionBudget>
{
public:
    static constexpr EObjectType Type = EObjectType::PodDisruptionBudget;

    TPodDisruptionBudget(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TStatus = NClient::NApi::NProto::TPodDisruptionBudgetStatus;
    static const TScalarAttributeSchema<TPodDisruptionBudget, TStatus> StatusSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TStatus>, Status);

    using TSpec = NClient::NApi::NProto::TPodDisruptionBudgetSpec;
    static const TScalarAttributeSchema<TPodDisruptionBudget, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);

    using TPodSetsAttribute = TOneToManyAttribute<TPodDisruptionBudget, TPodSet>;
    static const TPodSetsAttribute::TSchema PodSetsSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPodSetsAttribute, PodSets);

    // TODO(bidzilya): Remove in favor of the CAS.
    static const TTimestampAttributeSchema StatusUpdateTimestampSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TTimestampAttribute, StatusUpdateTimestamp);

    // Forbids all pod disruptions until the next synchronization.
    void FreezeUntilSync(const TString& message);

    void UpdateStatus(i32 allowedPodDisruptions, const TString& message);

    void AcceptDisruption(
        const TString& disruptionVerb,
        const TString& disruption,
        bool validate);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
