#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/ytlib/sequoia_client/transaction_options.h>

#include <yt/yt/ytlib/sequoia_client/proto/transaction_client.pb.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual void StartTransaction(NSequoiaClient::NProto::TReqStartTransaction* request) = 0;

    virtual NSequoiaClient::TSequoiaTransactionFeatures GetSequoiaTransactionFeatures() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISequoiaManager)

////////////////////////////////////////////////////////////////////////////////

ISequoiaManagerPtr CreateSequoiaManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
