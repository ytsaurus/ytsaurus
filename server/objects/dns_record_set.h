#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EDnsResourceRecordType,
    ((A)         (1))
    ((PTR)      (12))
    ((AAAA)     (28))
    ((SRV)      (33))
);

class TDnsRecordSet
    : public TObject
    , public NYT::TRefTracked<TDnsRecordSet>
{
public:
    static constexpr EObjectType Type = EObjectType::DnsRecordSet;

    TDnsRecordSet(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TSpec = NYP::NClient::NApi::NProto::TDnsRecordSetSpec;
    static const TScalarAttributeSchema<TDnsRecordSet, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

