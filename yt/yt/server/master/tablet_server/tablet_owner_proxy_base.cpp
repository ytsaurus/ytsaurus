#include "tablet_owner_proxy_base.h"

#include "private.h"
#include "tablet_owner_base.h"
#include "tablet_manager.h"
#include "tablet_base.h"

#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

namespace NYT::NTabletServer {

using namespace NTabletClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletServerLogger;

////////////////////////////////////////////////////////////////////////////////

void TTabletOwnerProxyBase::DoListSystemAttributes(
    std::vector<TAttributeDescriptor>* descriptors,
    bool showTabletAttributes)
{
    TBase::ListSystemAttributes(descriptors);

    const auto* table = GetThisImpl();
    const auto* trunkTable = table->GetTrunkNode();

    bool isExternal = table->IsExternal();

    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCount)
        .SetExternal(isExternal)
        .SetPresent(showTabletAttributes));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletState)
        .SetPresent(showTabletAttributes));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ActualTabletState)
        .SetPresent(showTabletAttributes));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ExpectedTabletState)
        .SetPresent(showTabletAttributes));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CurrentMountTransactionId)
        .SetPresent(showTabletAttributes && trunkTable->GetCurrentMountTransactionId()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::LastMountTransactionId)
        .SetPresent(showTabletAttributes && trunkTable->GetLastMountTransactionId()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCountByState)
        .SetExternal(isExternal)
        .SetPresent(showTabletAttributes)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCountByExpectedState)
        .SetExternal(isExternal)
        .SetPresent(showTabletAttributes)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletStatistics)
        .SetExternal(isExternal)
        .SetPresent(showTabletAttributes)
        .SetOpaque(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletErrorCount)
        .SetExternal(isExternal)
        .SetPresent(showTabletAttributes));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TabletCellBundle)
        .SetWritable(true)
        .SetPresent(IsObjectAlive(trunkTable->TabletCellBundle()))
        .SetReplicated(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::InMemoryMode)
        .SetReplicated(true)
        .SetWritable(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RemountNeededTabletCount)
        .SetExternal(isExternal));
}

bool TTabletOwnerProxyBase::DoGetBuiltinAttribute(
    TInternedAttributeKey key,
    IYsonConsumer* consumer,
    bool showTabletAttributes)
{
    const auto* table = GetThisImpl();
    const auto* trunkTable = table->GetTrunkNode();

    bool isExternal = table->IsExternal();

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    const auto& config = Bootstrap_->GetConfigManager()->GetConfig();

    auto validateTabletStatistics = [&] {
        TTabletStatistics oldFashionedStatistics;
        for (const auto* tablet : trunkTable->Tablets()) {
            oldFashionedStatistics += tablet->GetTabletStatistics();
        }

        auto ultraModernStatistics = trunkTable->GetTabletStatistics();

        if (oldFashionedStatistics == ultraModernStatistics) {
            return;
        }

        YT_LOG_ALERT("Tablet statistics mismatch (TableId: %v, OldStatistics: %v, NewStatistics: %v)",
            trunkTable->GetId(),
            ToString(oldFashionedStatistics, chunkManager),
            ToString(ultraModernStatistics, chunkManager));
    };

    if (showTabletAttributes && !isExternal && config->TabletManager->EnableAggressiveTabletStatisticsValidation) {
        validateTabletStatistics();
    }

    switch (key) {
        case EInternedAttributeKey::TabletCount:
            if (!showTabletAttributes || isExternal) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->Tablets().size());
            return true;

        case EInternedAttributeKey::TabletState:
            if (!showTabletAttributes) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetTabletState());
            return true;

        case EInternedAttributeKey::ActualTabletState:
            if (!showTabletAttributes) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetActualTabletState());
            return true;

        case EInternedAttributeKey::ExpectedTabletState:
            if (!showTabletAttributes) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetExpectedTabletState());
            return true;

        case EInternedAttributeKey::CurrentMountTransactionId:
            if (!showTabletAttributes || !trunkTable->GetCurrentMountTransactionId()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetCurrentMountTransactionId());
            return true;

        case EInternedAttributeKey::LastMountTransactionId:
            if (!showTabletAttributes || !trunkTable->GetLastMountTransactionId()) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetLastMountTransactionId());
            return true;

        case EInternedAttributeKey::TabletCountByState:
            if (!showTabletAttributes || isExternal) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->TabletCountByState());
            return true;

        case EInternedAttributeKey::TabletCountByExpectedState:
            if (!showTabletAttributes || isExternal) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->TabletCountByExpectedState());
            return true;

        case EInternedAttributeKey::TabletStatistics: {
            if (!showTabletAttributes || isExternal) {
                break;
            }

            if (config->TabletManager->EnableRelaxedTabletStatisticsValidation) {
                validateTabletStatistics();
            }

            BuildYsonFluently(consumer)
                .Value(New<TSerializableTabletStatistics>(
                    trunkTable->GetTabletStatistics(),
                    chunkManager));
            return true;
        }

        case EInternedAttributeKey::TabletErrorCount:
            if (!showTabletAttributes || isExternal) {
                break;
            }
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetTabletErrorCount());
            return true;

        case EInternedAttributeKey::TabletCellBundle:
            if (const auto& cellBundle = trunkTable->TabletCellBundle()) {
                BuildYsonFluently(consumer)
                    .Value(trunkTable->TabletCellBundle()->GetName());
                return true;
            } else {
                return false;
            }

        case EInternedAttributeKey::InMemoryMode:
            BuildYsonFluently(consumer)
                .Value(trunkTable->GetInMemoryMode());
            return true;

        case EInternedAttributeKey::RemountNeededTabletCount:
            if (isExternal) {
                break;
            }

            BuildYsonFluently(consumer)
                .Value(trunkTable->GetRemountNeededTabletCount());
            return true;

        default:
            break;
    }

    return TBase::GetBuiltinAttribute(key, consumer);
}

bool TTabletOwnerProxyBase::SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value)
{
    switch (key) {
        case EInternedAttributeKey::TabletCellBundle: {
            ValidateNoTransaction();

            auto name = ConvertTo<TString>(value);
            const auto& tabletManager = Bootstrap_->GetTabletManager();
            auto* cellBundle = tabletManager->GetTabletCellBundleByNameOrThrow(name, /*activeLifeStageOnly*/ true);

            auto* lockedTable = LockThisImpl();
            tabletManager->SetTabletCellBundle(lockedTable, cellBundle);

            return true;
        }

        case EInternedAttributeKey::InMemoryMode: {
            ValidateNoTransaction();

            auto* lockedTable = LockThisImpl();
            lockedTable->ValidateAllTabletsUnmounted("Cannot change table memory mode");

            auto inMemoryMode = ConvertTo<EInMemoryMode>(value);
            lockedTable->SetInMemoryMode(inMemoryMode);

            return true;
        }

        default:
            break;
    }

    return TBase::SetBuiltinAttribute(key, value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
