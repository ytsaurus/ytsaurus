#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt_proto/yt/client/chaos_client/proto/replication_card.pb.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TReplicationProgress& replicationProgress, NYson::IYsonConsumer* consumer);
void Serialize(const TReplicaHistoryItem& replicaHistoryItem, NYson::IYsonConsumer* consumer);
void Serialize(const TReplicaInfo& replicaInfo, NYson::IYsonConsumer* consumer);
void Serialize(const TReplicationCard& replicationCard, NYson::IYsonConsumer* consumer);

void Deserialize(TReplicationProgress& replicationProgress, NYTree::INodePtr node);
void Deserialize(TReplicaInfo& replicaInfo, NYTree::INodePtr node);
void Deserialize(TReplicationCard& replicationCard, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

void ToProto(NChaosClient::NProto::TReplicationProgress* protoReplicationProgress, const TReplicationProgress& replicationProgress);
void FromProto(TReplicationProgress* replicationProgress, const NChaosClient::NProto::TReplicationProgress& protoReplicationProgress);

void ToProto(NChaosClient::NProto::TReplicaInfo* protoReplicaInfo, const TReplicaInfo& replicaInfo, bool includeProgress = false, bool includeHistory = false);
void FromProto(TReplicaInfo* replicaInfo, const NChaosClient::NProto::TReplicaInfo& protoReplicaInfo);

void ToProto(NChaosClient::NProto::TReplicationCard* protoReplicationCard, const TReplicationCard& replicationCard);
void FromProto(TReplicationCard* replicationCard, const NChaosClient::NProto::TReplicationCard& protoReplicationCard);

void ToProto(NChaosClient::NProto::TReplicationCardToken* protoReplicationCardToken, const TReplicationCardToken& replicationCardToken);
void FromProto(TReplicationCardToken* replicationCardToken, const NChaosClient::NProto::TReplicationCardToken& protoReplicationCardToken);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
