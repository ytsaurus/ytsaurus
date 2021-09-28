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
    NChaosClient::TReplicationCardToken ReplicationCardToken;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateReplicationCardCommand
    : public TTypedCommand<NApi::TCreateReplicationCardOptions>
{
public:
    TCreateReplicationCardCommand();

private:
    NChaosClient::TReplicationCardToken ReplicationCardToken;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TGetReplicationCardCommand
    : public TTypedCommand<NApi::TGetReplicationCardOptions>
    , public TReplicationCardCommandBase
{
public:
    TGetReplicationCardCommand();

private:
    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCreateReplicationCardReplicaCommand
    : public TTypedCommand<NApi::TCreateReplicationCardReplicaOptions>
    , public TReplicationCardCommandBase
{
public:
    TCreateReplicationCardReplicaCommand();

private:
    NChaosClient::TReplicaInfo ReplicaInfo;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TRemoveReplicationCardReplicaCommand
    : public TTypedCommand<NApi::TRemoveReplicationCardReplicaOptions>
    , public TReplicationCardCommandBase
{
public:
    TRemoveReplicationCardReplicaCommand();

private:
    NChaosClient::TReplicaId ReplicaId;

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TAlterReplicationCardReplicaCommand
    : public TTypedCommand<NApi::TAlterReplicationCardReplicaOptions>
    , public TReplicationCardCommandBase
{
public:
    TAlterReplicationCardReplicaCommand();

private:
    NChaosClient::TReplicaId ReplicaId;

    virtual void DoExecute(ICommandContextPtr context) override;
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

    virtual void DoExecute(ICommandContextPtr context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
