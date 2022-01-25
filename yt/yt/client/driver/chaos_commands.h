#pragma once

#include "command.h"

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

class TReplicationCardCommandBase
    : public virtual NYTree::TYsonSerializableLite
{
public:
    TReplicationCardCommandBase();

protected:
    NChaosClient::TReplicationCardId ReplicationCardId;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateReplicationCardCommand
    : public TTypedCommand<NApi::TCreateReplicationCardOptions>
{
public:
    TCreateReplicationCardCommand();

private:
    NObjectClient::TCellId ChaosCellId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TUpdateReplicationProgressCommand
    : public TTypedCommand<NApi::TUpdateReplicationProgressOptions>
    , public TReplicationCardCommandBase
{
public:
    TUpdateReplicationProgressCommand();

private:
    NChaosClient::TReplicaId ReplicaId;

    void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
