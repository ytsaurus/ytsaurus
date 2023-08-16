#include "node.h"

#include "data_center.h"
#include "host.h"
#include "rack.h"
#include "node_statistics_helpers.h"
#include "node_tracker.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/cell_server/cell_base.h>
#include <yt/yt/server/master/cell_server/cell_bundle.h>

#include <yt/yt/server/master/chunk_server/chunk_location.h>
#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/medium.h>

#include <yt/yt/server/master/node_tracker_server/config.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>
#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/orchid/orchid_ypath_service.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/static_service_dispatcher.h>

namespace NYT::NNodeTrackerServer {

using namespace NCellarClient;
using namespace NChunkClient;
using namespace NMaintenanceTrackerServer;
using namespace NNodeTrackerClient::NProto;
using namespace NNodeTrackerClient;
using namespace NObjectServer;
using namespace NOrchid;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeProxy
    : public TNonversionedObjectProxyBase<TNode>
    , public virtual TStaticServiceDispatcher
{
public:
    TClusterNodeProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TNode* node)
        : TNonversionedObjectProxyBase(bootstrap, metadata, node)
    {
        RegisterService(
            "orchid",
            BIND(&TClusterNodeProxy::CreateOrchidService, Unretained(this)));
    }

private:
    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TNonversionedObjectProxyBase::ListSystemAttributes(descriptors);

        const auto* node = GetThisImpl();

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Banned)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Decommissioned)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DisableWriteSessions)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DisableSchedulerJobs)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DisableTabletCells)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MaintenanceRequests)
            .SetWritable(false));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Rack)
            .SetPresent(node->GetRack())
            .SetWritable(true)
            .SetRemovable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Host)
            .SetPresent(node->GetHost())
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(EInternedAttributeKey::DataCenter);
        descriptors->push_back(EInternedAttributeKey::State);
        descriptors->push_back(EInternedAttributeKey::MulticellStates);
        descriptors->push_back(EInternedAttributeKey::RegistrationPending);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::UserTags)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(EInternedAttributeKey::Tags);
        descriptors->push_back(EInternedAttributeKey::LastSeenTime);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Annotations)
            .SetPresent(static_cast<bool>(node->GetAnnotations())));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Version));
        bool isGood = node->GetLocalState() == ENodeState::Registered || node->GetLocalState() == ENodeState::Online;
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RegisterTime)
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LeaseTransactionId)
            .SetPresent(isGood && node->GetLeaseTransaction()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Statistics)
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Full)
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Addresses)
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Alerts)
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::AlertCount)
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Flavors)
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletSlots)
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Cellars)
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::IOWeights)
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ResourceUsage)
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ResourceLimits)
            .SetPresent(isGood));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ResourceLimitsOverrides)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChunkReplicaCount)
            .SetPresent(isGood && Bootstrap_->GetMulticellManager()->IsPrimaryMaster()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DestroyedChunkReplicaCount)
            .SetPresent(isGood && Bootstrap_->GetMulticellManager()->IsPrimaryMaster()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PushReplicationQueueSize)
            .SetPresent(isGood && Bootstrap_->GetMulticellManager()->IsPrimaryMaster()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PullReplicationQueueSize)
            .SetPresent(isGood && Bootstrap_->GetMulticellManager()->IsPrimaryMaster()));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::PullReplicationChunkCount)
            .SetPresent(isGood && Bootstrap_->GetMulticellManager()->IsPrimaryMaster()));
        descriptors->push_back(EInternedAttributeKey::ConsistentReplicaPlacementTokenCount);
        descriptors->push_back(EInternedAttributeKey::ChunkLocations);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::JobProxyVersion)
            .SetPresent(node->JobProxyVersion().has_value()));
        descriptors->push_back(EInternedAttributeKey::UseImaginaryChunkLocations);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* node = GetThisImpl();
        auto state = node->GetLocalState();
        bool isGood = state == ENodeState::Registered || state == ENodeState::Online;

        switch (key) {
            case EInternedAttributeKey::Banned:
                BuildYsonFluently(consumer)
                    .Value(node->IsBanned());
                return true;

            case EInternedAttributeKey::Decommissioned:
                BuildYsonFluently(consumer)
                    .Value(node->IsDecommissioned());
                return true;

            case EInternedAttributeKey::DisableWriteSessions:
                BuildYsonFluently(consumer)
                    .Value(node->AreWriteSessionsDisabled());
                return true;

            case EInternedAttributeKey::DisableSchedulerJobs:
                BuildYsonFluently(consumer)
                    .Value(node->AreSchedulerJobsDisabled());
                return true;

            case EInternedAttributeKey::DisableTabletCells:
                BuildYsonFluently(consumer)
                    .Value(node->AreTabletCellsDisabled());
                return true;

            case EInternedAttributeKey::MaintenanceRequests: {
                TCompactVector<std::pair<TMaintenanceId, TMaintenanceRequest>, TypicalMaintenanceRequestCount> requests(
                    node->MaintenanceRequests().begin(),
                    node->MaintenanceRequests().end());
                Sort(requests, [] (const auto& lhs, const auto& rhs) {
                    return lhs.second.Type < rhs.second.Type;
                });
                BuildYsonFluently(consumer)
                    .DoMapFor(requests, [] (TFluentMap map, const auto& request) {
                        map.Item(ToString(request.first))
                            .BeginMap()
                                .Item("user").Value(request.second.User)
                                .Item("comment").Value(request.second.Comment)
                                .Item("timestamp").Value(request.second.Timestamp)
                                .Item("type").Value(request.second.Type)
                            .EndMap();
                    });
                return true;
            }

            case EInternedAttributeKey::Rack:
                if (!node->GetRack()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(node->GetRack()->GetName());
                return true;

            case EInternedAttributeKey::Host:
                if (!node->GetHost()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(node->GetHost()->GetName());
                return true;

            case EInternedAttributeKey::DataCenter:
                if (!node->GetRack() || !node->GetRack()->GetDataCenter()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(node->GetRack()->GetDataCenter()->GetName());
                return true;

            case EInternedAttributeKey::State: {
                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                auto state = multicellManager->IsPrimaryMaster()
                    ? node->GetAggregatedState()
                    : node->GetLocalState();
                BuildYsonFluently(consumer)
                    .Value(state);
                return true;
            }

            case EInternedAttributeKey::RegistrationPending:
                BuildYsonFluently(consumer)
                    .Value(node->GetRegistrationPending());
                return true;

            case EInternedAttributeKey::Annotations: {
                if (!node->GetAnnotations()) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(node->GetAnnotations());
                return true;
            }

            case EInternedAttributeKey::Version: {
                BuildYsonFluently(consumer)
                    .Value(node->GetVersion());
                return true;
            }

            case EInternedAttributeKey::MulticellStates:
                BuildYsonFluently(consumer)
                    .DoMapFor(node->MulticellDescriptors(), [] (TFluentMap fluent, const auto& pair) {
                        fluent.Item(ToString(pair.first)).Value(pair.second.State);
                    });
                return true;

            case EInternedAttributeKey::UserTags:
                BuildYsonFluently(consumer)
                    .Value(node->UserTags());
                return true;

            case EInternedAttributeKey::Tags:
                BuildYsonFluently(consumer)
                    .Value(node->Tags());
                return true;

            case EInternedAttributeKey::LastSeenTime:
                BuildYsonFluently(consumer)
                    .Value(node->GetLastSeenTime());
                return true;

            case EInternedAttributeKey::RegisterTime:
                if (!isGood) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(node->GetRegisterTime());
                return true;

            case EInternedAttributeKey::LeaseTransactionId:
                if (!isGood || !node->GetLeaseTransaction()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(node->GetLeaseTransaction()->GetId());
                return true;

            case EInternedAttributeKey::Statistics: {
                if (!isGood) {
                    break;
                }

                // XXX(gritukan): Should we look at flavours here?
                // Probably it's better not to send data node statistics of the tablet node.
                const auto& chunkManager = Bootstrap_->GetChunkManager();
                const auto& clusterNodeStatistics = node->ClusterNodeStatistics();
                const auto& dataNodeStatistics = node->DataNodeStatistics();
                const auto& execNodeStatistics = node->ExecNodeStatistics();

                auto serializeChunkLocationStatistics = [&] (TFluentList fluent, const TChunkLocationStatistics& statistics) {
                    fluent
                        .Item().BeginMap()
                            .Item("location_uuid").Value(FromProto<TChunkLocationUuid>(statistics.location_uuid()))
                            .Do([&] (auto fluent) {
                                Serialize(statistics, fluent, chunkManager);
                            })
                        .EndMap();
                };

                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("total_available_space").Value(dataNodeStatistics.total_available_space())
                        .Item("total_used_space").Value(dataNodeStatistics.total_used_space())
                        .Item("total_stored_chunk_count").Value(dataNodeStatistics.total_stored_chunk_count())
                        .Item("total_cached_chunk_count").Value(dataNodeStatistics.total_cached_chunk_count())
                        .Item("total_session_count").Value(node->GetTotalSessionCount())
                        .Item("full").Value(dataNodeStatistics.full())
                        // COMPAT(gritukan): Drop it in favour of `chunk_locations'.
                        .Item("locations").DoListFor(dataNodeStatistics.chunk_locations(), serializeChunkLocationStatistics)
                        .Item("chunk_locations").DoListFor(dataNodeStatistics.chunk_locations(), serializeChunkLocationStatistics)
                        .Item("slot_locations").DoListFor(execNodeStatistics.slot_locations(), [&] (TFluentList fluent, const TSlotLocationStatistics& statistics) {
                            fluent
                                .Item().BeginMap()
                                    .Do([&] (auto fluent) {
                                        Serialize(statistics, fluent, chunkManager);
                                    })
                                .EndMap();
                        })
                        .Item("media").DoMapFor(dataNodeStatistics.media(), [&] (TFluentMap fluent, const TMediumStatistics& mediumStatistics) {
                            auto mediumIndex = mediumStatistics.medium_index();
                            const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                            if (!IsObjectAlive(medium)) {
                                return;
                            }
                            fluent
                                .Item(medium->GetName()).BeginMap()
                                    .Item("io_weight").Value(mediumStatistics.io_weight())
                                .EndMap();
                        })
                        .Item("memory").BeginMap()
                            .Item("total").BeginMap()
                                .Item("used").Value(clusterNodeStatistics.memory().total_used())
                                .Item("limit").Value(clusterNodeStatistics.memory().total_limit())
                            .EndMap()
                            .DoFor(clusterNodeStatistics.memory().categories(), [] (TFluentMap fluent, const TMemoryStatistics::TCategory& category) {
                                fluent.Item(FormatEnum(FromProto<EMemoryCategory>(category.type())))
                                    .BeginMap()
                                        .DoIf(category.has_limit(), [&] (TFluentMap fluent) {
                                            fluent.Item("limit").Value(category.limit());
                                        })
                                        .Item("used").Value(category.used())
                                    .EndMap();
                            })
                        .EndMap()
                        .Item("network").BeginMap()
                            .DoFor(clusterNodeStatistics.network(), [] (TFluentMap fluent, const TNetworkStatistics& statistics) {
                                fluent.Item(statistics.network())
                                    .BeginMap()
                                        .Item("throttling_reads").Value(statistics.throttling_reads())
                                    .EndMap();
                            })
                        .EndMap()
                        .DoIf(clusterNodeStatistics.has_cpu(), [&] (TFluentMap fluent) {
                            fluent
                                .Item("cpu").BeginMap()
                                    .Item("total_used").Value(clusterNodeStatistics.cpu().total_used())
                                    .DoIf(clusterNodeStatistics.cpu().has_total_limit(), [&] (TFluentMap fluent) {
                                        fluent
                                            .Item("total_limit").Value(clusterNodeStatistics.cpu().total_limit());
                                    })
                                    .DoIf(clusterNodeStatistics.cpu().has_total_guarantee(), [&] (TFluentMap fluent) {
                                        fluent
                                            .Item("total_guarantee").Value(clusterNodeStatistics.cpu().total_guarantee());
                                    })
                                    .Item("tablet_slots").Value(clusterNodeStatistics.cpu().tablet_slots())
                                    .Item("dedicated").Value(clusterNodeStatistics.cpu().dedicated())
                                    .Item("jobs").Value(clusterNodeStatistics.cpu().jobs())
                                .EndMap();
                        })
                    .EndMap();
                return true;
            }

            case EInternedAttributeKey::Full:
                if (!isGood) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(node->DataNodeStatistics().full());
                return true;

            case EInternedAttributeKey::Alerts:
                if (!isGood) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(node->Alerts());
                return true;

            case EInternedAttributeKey::AlertCount:
                if (!isGood) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(node->Alerts().size());
                return true;

            case EInternedAttributeKey::Addresses:
                if (!isGood) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(node->GetNodeAddresses());
                return true;

            case EInternedAttributeKey::Flavors:
                if (!isGood) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(node->Flavors());
                return true;

            case EInternedAttributeKey::TabletSlots:
                if (!isGood || !node->FindCellar(ECellarType::Tablet)) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Do(BIND(&TClusterNodeProxy::BuildYsonCellar, node->GetCellar(ECellarType::Tablet)));
                return true;

            case EInternedAttributeKey::Cellars:
                if (!isGood) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .DoMapFor(node->Cellars(), [] (TFluentMap fluent, const auto& it) {
                        fluent
                            .Item(CamelCaseToUnderscoreCase(ToString(it.first)))
                            .Do(BIND(&TClusterNodeProxy::BuildYsonCellar, it.second));
                    });
                return true;

            case EInternedAttributeKey::IOWeights: {
                if (!isGood) {
                    break;
                }

                const auto& chunkManager = Bootstrap_->GetChunkManager();
                BuildYsonFluently(consumer)
                    .DoMapFor(node->IOWeights().begin(), node->IOWeights().end(), [&] (
                        auto fluent,
                        auto item)
                    {
                        auto* medium = chunkManager->FindMediumByIndex(item->first);
                        if (IsObjectAlive(medium)) {
                            fluent
                                .Item(medium->GetName())
                                .Value(item->second);
                        }
                    });

                return true;
            }

            case EInternedAttributeKey::ResourceUsage:
                if (!isGood) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(node->ResourceUsage());
                return true;

            case EInternedAttributeKey::ResourceLimits:
                if (!isGood) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(node->ResourceLimits());
                return true;

            case EInternedAttributeKey::ResourceLimitsOverrides:
                BuildYsonFluently(consumer)
                    .Value(node->ResourceLimitsOverrides());
                return true;

            case EInternedAttributeKey::ChunkReplicaCount: {
                if (!isGood) {
                    break;
                }

                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                if (!multicellManager->IsPrimaryMaster()) {
                    break;
                }

                const auto statistics = node->ComputeClusterStatistics();

                const auto& chunkManager = Bootstrap_->GetChunkManager();
                BuildYsonFluently(consumer)
                    .DoMapFor(chunkManager->Media(), [&] (
                        TFluentMap fluent,
                        const std::pair<const TGuid, NChunkServer::TMedium*>& pair)
                    {
                        const auto* medium = pair.second;
                        if (IsObjectAlive(medium)) {
                            fluent
                                .Item(medium->GetName())
                                .Value(GetOrDefault(statistics.ChunkReplicaCount, medium->GetIndex()));
                        }
                    });
                return true;
            }

            case EInternedAttributeKey::DestroyedChunkReplicaCount: {
                if (!isGood) {
                    break;
                }

                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                if (!multicellManager->IsPrimaryMaster()) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(node->ComputeClusterStatistics().DestroyedChunkReplicaCount);
                return true;
            }

            case EInternedAttributeKey::PushReplicationQueueSize: {
                RequireLeader();

                if (!isGood) {
                    break;
                }

                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                if (!multicellManager->IsPrimaryMaster()) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(node->ComputeClusterStatistics().ChunkPushReplicationQueuesSize);
                return true;
            }

            case EInternedAttributeKey::PullReplicationQueueSize: {
                RequireLeader();

                if (!isGood) {
                    break;
                }

                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                if (!multicellManager->IsPrimaryMaster()) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(node->ComputeClusterStatistics().ChunkPullReplicationQueuesSize);
                return true;
            }

            case EInternedAttributeKey::PullReplicationChunkCount: {
                RequireLeader();

                if (!isGood) {
                    break;
                }

                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                if (!multicellManager->IsPrimaryMaster()) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(node->ComputeClusterStatistics().PullReplicationChunkCount);
                return true;
            }

            case EInternedAttributeKey::ConsistentReplicaPlacementTokenCount: {
                if (node->GetLocalState() != ENodeState::Online) {
                    break;
                }

                if (node->IsDecommissioned()) {
                    break;
                }

                const auto& chunkManager = Bootstrap_->GetChunkManager();
                BuildYsonFluently(consumer)
                    .DoMapFor(
                        node->ConsistentReplicaPlacementTokenCount(),
                        [&] (TFluentMap fluent, const auto& pair) {

                        auto* medium = chunkManager->FindMediumByIndex(pair.first);
                        if (IsObjectAlive(medium)) {
                            fluent.Item(medium->GetName()).Value(pair.second);
                        }
                    });
                return true;
            }

            case EInternedAttributeKey::ChunkLocations: {
                const auto& chunkManager = Bootstrap_->GetChunkManager();

                BuildYsonFluently(consumer)
                    .DoMapFor(node->RealChunkLocations(), [&] (TFluentMap fluent, const auto* location) {
                        fluent
                            .Item(ToString(location->GetUuid()))
                            .BeginMap()
                                .Do([&] (auto fluent) {
                                    Serialize(location->Statistics(), fluent, chunkManager);
                                })
                            .EndMap();
                    });
                return true;
            }

            case EInternedAttributeKey::JobProxyVersion: {
                const auto& jobProxyVersion = node->JobProxyVersion();
                if (!jobProxyVersion) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .Value(jobProxyVersion);
                return true;
            }

            case EInternedAttributeKey::UseImaginaryChunkLocations: {
                BuildYsonFluently(consumer)
                    .Value(GetThisImpl()->UseImaginaryChunkLocations());

                return true;
            }

            default:
                break;
        }

        return TNonversionedObjectProxyBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        auto* node = GetThisImpl();
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        switch (key) {
            case EInternedAttributeKey::Banned: [[fallthrough]];
            case EInternedAttributeKey::Decommissioned: [[fallthrough]];
            case EInternedAttributeKey::DisableTabletCells: [[fallthrough]];
            case EInternedAttributeKey::DisableSchedulerJobs: [[fallthrough]];
            case EInternedAttributeKey::DisableWriteSessions:
                // COMPAT(kvk1920): In near future this attribute will not be writable.
                SetMaintenanceFlag(key, ConvertTo<bool>(value));
                return true;

            case EInternedAttributeKey::Rack: {
                auto rackName = ConvertTo<TString>(value);
                auto* host = node->GetHost();
                if (!host) {
                    THROW_ERROR_EXCEPTION("Cannot set rack for a node without host");
                }
                auto* rack = nodeTracker->GetRackByNameOrThrow(rackName);
                nodeTracker->SetHostRack(host, rack);
                return true;
            }

            case EInternedAttributeKey::Host: {
                auto hostName = ConvertTo<TString>(value);
                auto* host = nodeTracker->GetHostByNameOrThrow(hostName);
                nodeTracker->SetNodeHost(node, host);
                return true;
            }

            case EInternedAttributeKey::ResourceLimitsOverrides:
                node->ResourceLimitsOverrides() = ConvertTo<TNodeResourceLimitsOverrides>(value);
                return true;

            case EInternedAttributeKey::UserTags:
                nodeTracker->SetNodeUserTags(node, ConvertTo<std::vector<TString>>(value));
                return true;

            default:
                break;
        }

        return TNonversionedObjectProxyBase::SetBuiltinAttribute(key, value);
    }

    bool RemoveBuiltinAttribute(TInternedAttributeKey key) override
    {
        auto* node = GetThisImpl();
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();

        switch (key) {
            case EInternedAttributeKey::Rack: {
                if (auto* host = node->GetHost()) {
                    nodeTracker->SetHostRack(host, nullptr);
                }
                return true;
            }

            default:
                break;
        }

        return false;
    }

    void ValidateRemoval() override
    {
        const auto* node = GetThisImpl();
        if (node->GetLocalState() != ENodeState::Offline) {
            THROW_ERROR_EXCEPTION("Cannot remove node since it is not offline");
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto* location : node->RealChunkLocations()) {
            objectManager->ValidateObjectLifeStage(location);
        }
    }

    IYPathServicePtr CreateOrchidService()
    {
        auto* node = GetThisImpl<TNode>();
        if (!IsObjectAlive(node)) {
            THROW_ERROR_EXCEPTION("Node is not alive");
        }

        auto nodeAddresses = node->GetAddressesOrThrow(EAddressType::InternalRpc);

        // TODO(max42): make customizable.
        constexpr TDuration timeout = TDuration::Seconds(60);

        return CreateOrchidYPathService(TOrchidOptions{
            .Channel = Bootstrap_->GetNodeChannelFactory()->CreateChannel(std::move(nodeAddresses)),
            .Timeout = timeout,
        });
    }

    static void BuildYsonCellar(const TNode::TCellar& cellar, TFluentAny fluent)
    {
        fluent
            .DoListFor(cellar, [] (TFluentList fluent, const TNode::TCellSlot& slot) {
                fluent
                    .Item().BeginMap()
                    .Item("state").Value(slot.PeerState)
                    .DoIf(slot.Cell, [&](TFluentMap fluent) {
                        fluent
                            .Item("cell_id").Value(slot.Cell->GetId())
                            .Item("peer_id").Value(slot.PeerId)
                            .Item("cell_bundle").Value(slot.Cell->CellBundle()->GetName())
                            // COMPAT(savrus)
                            .Item("tablet_cell_bundle").Value(slot.Cell->CellBundle()->GetName());
                    })
                    .EndMap();
            });
    }

    void SetMaintenanceFlag(TInternedAttributeKey key, bool value)
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        const auto& config = configManager->GetConfig()->NodeTracker;
        THROW_ERROR_EXCEPTION_IF(config->ForbidMaintenanceAttributeWrites,
            "Node attribute %Qv is deprecated; use add_maintenance/remove_maintenance commands instead",
            key.Unintern());

        auto getMaintenanceType = [=] {
            switch (key) {
                case EInternedAttributeKey::Banned:
                    return EMaintenanceType::Ban;
                case EInternedAttributeKey::Decommissioned:
                    return EMaintenanceType::Decommission;
                case EInternedAttributeKey::DisableSchedulerJobs:
                    return EMaintenanceType::DisableSchedulerJobs;
                case EInternedAttributeKey::DisableTabletCells:
                    return EMaintenanceType::DisableTabletCells;
                case EInternedAttributeKey::DisableWriteSessions:
                    return EMaintenanceType::DisableWriteSessions;
                default:
                    YT_ABORT();
            }
        };

        auto type = getMaintenanceType();

        auto* node = GetThisImpl();
        bool maintenanceUpdated;
        if (value) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            auto* user = securityManager->GetAuthenticatedUser();
            auto* hydraContext = NHydra::GetCurrentHydraContext();
            auto timestamp = hydraContext->GetTimestamp();
            maintenanceUpdated = node->SetMaintenanceFlag(type, user ? user->GetName() : "", timestamp);
        } else {
            maintenanceUpdated = node->ClearMaintenanceFlag(type);
        }

        if (maintenanceUpdated) {
            const auto& nodeTracker = Bootstrap_->GetNodeTracker();
            nodeTracker->OnNodeMaintenanceUpdated(node, type);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateClusterNodeProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TNode* node)
{
    return New<TClusterNodeProxy>(bootstrap, metadata, node);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer

