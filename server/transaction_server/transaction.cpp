#include "transaction.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/serialize.h>

#include <yt/server/chunk_server/chunk_owner_base.h>

#include <yt/server/security_server/account.h>
#include <yt/server/security_server/subject.h>

#include <yt/core/misc/string.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NTransactionServer {

using namespace NYTree;
using namespace NYson;
using namespace NCellMaster;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

void TTransaction::TExportEntry::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Object);
    Persist(context, DestinationCellTag);
}

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(const TTransactionId& id)
    : TTransactionBase(id)
    , Parent_(nullptr)
    , StartTime_(TInstant::Zero())
    , Acd_(this)
{ }

void TTransaction::Save(NCellMaster::TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);
    TTransactionBase::Save(context);

    using NYT::Save;
    Save(context, GetPersistentState());
    Save(context, Timeout_);
    Save(context, Title_);
    Save(context, SecondaryCellTags_);
    Save(context, NestedTransactions_);
    Save(context, Parent_);
    Save(context, StartTime_);
    Save(context, StagedObjects_);
    Save(context, ExportedObjects_);
    Save(context, ImportedObjects_);
    Save(context, LockedNodes_);
    Save(context, Locks_);
    Save(context, BranchedNodes_);
    Save(context, StagedNodes_);
    Save(context, AccountResourceUsage_);
    Save(context, Acd_);
    Save(context, PrerequisiteTransactions_);
    Save(context, DependentTransactions_);
}

void TTransaction::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);
    // COMPAT(babenko)
    if (context.GetVersion() >= 500) {
        TTransactionBase::Load(context);
    }

    using NYT::Load;
    Load(context, State_);
    Load(context, Timeout_);
    // COMPAT(shakurov)
    if (context.GetVersion() < 700) {
        Load<bool>(context); // drop AccountingEnabled_
    }
    Load(context, Title_);
    Load(context, SecondaryCellTags_);
    Load(context, NestedTransactions_);
    Load(context, Parent_);
    Load(context, StartTime_);
    Load(context, StagedObjects_);
    Load(context, ExportedObjects_);
    Load(context, ImportedObjects_);
    Load(context, LockedNodes_);
    Load(context, Locks_);
    Load(context, BranchedNodes_);
    Load(context, StagedNodes_);
    Load(context, AccountResourceUsage_);
    Load(context, Acd_);
    // COMPAT(shakurov)
    if (context.GetVersion() >= 702 && context.GetVersion() < 804) {
        Load<bool>(context); // drop System_
    }
    // COMPAT(babenko)
    if (context.GetVersion() >= 706) {
        Load(context, PrerequisiteTransactions_);
        Load(context, DependentTransactions_);
    }
}

void TTransaction::RecomputeResourceUsage()
{
    AccountResourceUsage_.clear();

    for (auto* node : BranchedNodes_) {
        AddNodeResourceUsage(node, false);
    }
    for (auto* node : StagedNodes_) {
        AddNodeResourceUsage(node, true);
    }
}

void TTransaction::AddNodeResourceUsage(const NCypressServer::TCypressNodeBase* node, bool staged)
{
    if (node->IsExternal()) {
        return;
    }

    auto* account = node->GetAccount();
    AccountResourceUsage_[account] += node->GetDeltaResourceUsage();
}

TYsonString TTransaction::GetErrorDescription() const
{
    auto customAttributes = CreateEphemeralAttributes();
    auto copyCustomAttribute = [&] (const TString& key) {
        if (!Attributes_) {
            return;
        }
        const auto& attributeMap = Attributes_->Attributes();
        auto it = attributeMap.find(key);
        if (it == attributeMap.end()) {
            return;
        }
        customAttributes->SetYson(it->first, it->second);
    };
    copyCustomAttribute("operation_id");
    copyCustomAttribute("operation_title");

    return BuildYsonStringFluently()
        .BeginMap()
            .Item("id").Value(Id_)
            .Item("start_time").Value(StartTime_)
            .Item("owner").Value(Acd_.GetOwner()->GetName())
            .DoIf(Timeout_.HasValue(), [&] (TFluentMap fluent) {
                fluent
                    .Item("timeout").Value(*Timeout_);
            })
            .DoIf(Title_.HasValue(), [&] (TFluentMap fluent) {
                fluent
                    .Item("title").Value(*Title_);
            })
            .DoIf(Parent_ != nullptr, [&] (TFluentMap fluent) {
                fluent
                    .Item("parent").Value(Parent_->GetErrorDescription());
            })
            .Items(*customAttributes)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT

