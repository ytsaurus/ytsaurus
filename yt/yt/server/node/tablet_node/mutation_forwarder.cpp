#include "mutation_forwarder.h"

#include "tablet_manager.h"
#include "tablet.h"

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/mailbox.h>

#include <google/protobuf/message.h>

namespace NYT::NTabletNode {

using namespace NHiveServer;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TMutationForwarder
    : public IMutationForwarder
{
public:
    TMutationForwarder(
        TWeakPtr<TTabletManager> tabletManager,
        IHiveManagerPtr hiveManager)
        : TabletManager_(std::move(tabletManager))
        , HiveManager_(std::move(hiveManager))
    { }

    void MaybeForwardMutationToSiblingServant(
        TTabletId tabletId,
        const ::google::protobuf::Message& message) override
    {
        auto tabletManager = TabletManager_.Lock();
        if (!tabletManager) {
            return;
        }

        const auto* tablet = tabletManager->FindTablet(tabletId);
        if (!tablet) {
            return;
        }

        // TODO(ifsmirnov, YT-17317): this is the dummy testing-only code. It will be replaced
        // by actual implementation when smooth tablet movement comes. Should work in
        // local yt and integration tests.
        if (CellTagFromId(tabletId) != 1 && CellTagFromId(tabletId) != 0xa) {
            return;
        }

        if (message.GetTypeName() == "NYT.NTabletNode.NProto.TReqRemountTablet") {
            if (tablet->GetRemountCount() % 2 == 0) {
                auto cellId = HiveManager_->GetSelfCellId();
                auto* mailbox = HiveManager_->GetOrCreateCellMailbox(cellId);
                HiveManager_->PostMessage(mailbox, message);
            }
        }
    }

private:
    const TWeakPtr<TTabletManager> TabletManager_;
    const IHiveManagerPtr HiveManager_;
};

////////////////////////////////////////////////////////////////////////////////

IMutationForwarderPtr CreateMutationForwarder(
    TWeakPtr<TTabletManager> tabletManager,
    IHiveManagerPtr hiveManager)
{
    return New<TMutationForwarder>(
        std::move(tabletManager),
        std::move(hiveManager));
}

IMutationForwarderPtr CreateDummyMutationForwarder()
{
    struct TDummyMutationForwarder
        : public IMutationForwarder
    {
        void MaybeForwardMutationToSiblingServant(TTabletId, const ::google::protobuf::Message&) override
        { }
    };

    return New<TDummyMutationForwarder>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
