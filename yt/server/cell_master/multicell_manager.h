#pragma once

#include "public.h"

#include <core/misc/ref.h>

#include <core/rpc/public.h>

#include <ytlib/election/public.h>

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TMulticellManager
    : public TRefCounted
{
public:
    TMulticellManager(
        TBootstrap* bootstrap,
        TCellMasterConfigPtr config);
    ~TMulticellManager();

    void Initialize();
    void Start();

    bool IsPrimaryMaster() const;
    bool IsSecondaryMaster() const;
    bool IsMulticell() const;

    const NElection::TCellId& GetCellId() const;
    NObjectClient::TCellTag GetCellTag() const;

    const NElection::TCellId& GetPrimaryCellId() const;
    NObjectClient::TCellTag GetPrimaryCellTag() const;

    const std::vector<NObjectClient::TCellTag>& GetSecondaryCellTags() const;

    NElection::TCellConfigPtr GetCellConfig() const;
    NElection::TPeerId GetPeerId() const;

    void PostToPrimaryMaster(
        const ::google::protobuf::MessageLite& requestMessage,
        bool reliable = true);

    void PostToSecondaryMasters(
        NRpc::IClientRequestPtr request,
        bool reliable = true);
    void PostToSecondaryMasters(
        const NObjectClient::TObjectId& objectId,
        NRpc::IServiceContextPtr context,
        bool reliable = true);
    void PostToSecondaryMasters(
        TSharedRefArray requestMessage,
        bool reliable = true);
    void PostToSecondaryMasters(
        const ::google::protobuf::MessageLite& requestMessage,
        bool reliable = true);

private:
    class TPart;
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TMulticellManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
