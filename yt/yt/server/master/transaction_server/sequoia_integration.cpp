#include "sequoia_integration.h"

#include "helpers.h"

#include <yt/yt/server/master/transaction_server/proto/transaction_manager.pb.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt/server/master/sequoia_server/config.h>

#include <yt/yt/server/lib/transaction_server/helpers.h>
#include <yt/yt/server/lib/transaction_server/private.h>

#include <yt/yt/ytlib/cypress_transaction_client/proto/cypress_transaction_service.pb.h>

#include <yt/yt/ytlib/transaction_client/action.h>

#include <yt/yt/ytlib/sequoia_client/client.h>
#include <yt/yt/ytlib/sequoia_client/helpers.h>
#include <yt/yt/ytlib/sequoia_client/table_descriptor.h>
#include <yt/yt/ytlib/sequoia_client/transaction.h>

#include <yt/yt/ytlib/sequoia_client/records/dependent_transactions.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transactions.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_descendants.record.h>
#include <yt/yt/ytlib/sequoia_client/records/transaction_replicas.record.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/config.h>

#include <yt/yt_proto/yt/core/ytree/proto/attributes.pb.h>

namespace NYT::NTransactionServer {

using namespace NApi;
using namespace NCellMaster;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NSequoiaClient;
using namespace NTableClient;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

const auto& Logger = TransactionServerLogger;

////////////////////////////////////////////////////////////////////////////////

// Helpers.

template <class T>
std::vector<T> MakeSortedAndUnique(std::vector<T>&& items)
{
    SortUnique(items);
    return items;
}

std::vector<NRecords::TTransactionKey> ToTransactionKeys(
    const std::vector<TTransactionId>& transactionIds)
{
    std::vector<NRecords::TTransactionKey> keys(transactionIds.size());
    std::transform(
        transactionIds.begin(),
        transactionIds.end(),
        keys.begin(),
        [] (auto transactionId) -> NRecords::TTransactionKey {
            return {.TransactionId = transactionId};
        });
    return keys;
}

void ValidateTransactionAncestors(const NRecords::TTransaction& record)
{
    auto isNested = TypeFromId(record.Key.TransactionId) == EObjectType::NestedTransaction;
    auto hasAncestors = !record.AncestorIds.empty();
    THROW_ERROR_EXCEPTION_IF(isNested != hasAncestors,
        NSequoiaClient::EErrorCode::SequoiaTableCorrupted,
        "Sequoia table %Qv is corrupted: %v",
        ITableDescriptor::Get(ESequoiaTable::Transactions)->GetTableName(),
        isNested
            ? "transaction is nested but its ancestor list is empty"
            : "transaction is topmost but its ancestor list is not empty");
}

void ValidateTransactionAncestors(
    const std::vector<std::optional<NRecords::TTransaction>>& records)
{
    for (const auto& record : records) {
        ValidateTransactionAncestors(*record);
    }
}

void ValidateAllTransactionsExist(
    const std::vector<std::optional<NRecords::TTransaction>>& records)
{
    for (const auto& record : records) {
        if (!record) {
            // TODO(kvk1920): more verbose message (e.g. list all transactions).
            THROW_ERROR_EXCEPTION(
                NSequoiaClient::EErrorCode::SequoiaTableCorrupted,
                "Sequoia table %Qv is corrupted: some transactions are mentioned as ancestors but "
                "they are missing at Sequoia table",
                ITableDescriptor::Get(ESequoiaTable::Transactions)->GetTableName());
        }
    }
}

TSelectRowsQuery BuildSelectByTransactionIds(const auto& transactions, const auto& getId)
{
    YT_ASSERT(!transactions.empty());

    TStringBuilder builder;
    auto it = transactions.begin();
    builder.AppendFormat("transaction_id in (%Qv", getId(*it++));
    while (it != transactions.end()) {
        builder.AppendFormat(", %Qv", getId(*it++));
    }
    builder.AppendChar(')');
    return {.WhereConjuncts = {builder.Flush()}};
}

////////////////////////////////////////////////////////////////////////////////

// The common case is the lazy replication from transaction coordinator which is
// initiated on foreign cell. In this case destination cell is the only
// destination, thus typical count is 1.
constexpr int TypicalTransactionReplicationDestinationCellCount = 1;
using TTransactionReplicationDestinationCellTagList =
    TCompactVector<TCellTag, TypicalTransactionReplicationDestinationCellCount>;

//! This class is responsible for instantiation of transactions' replicas on
//! foreign cells and modification of "transaction_replicas" Sequoia table.
/*!
 *  It is not responsible for neither transaction hierarchy handling nor
 *  transaction coordinator's state modification. This class is used as:
 *    -  part of complete transaction replication;
 *    -  fast path for explicitly requested replication on transaction start.
 *  This class is designed to be used locally (e.g. it assumes that Sequoia
 *  transaction won't be destroyed during lifetime of this class).
 */
class TSimpleTransactionReplicator
{
public:
    explicit TSimpleTransactionReplicator(ISequoiaTransaction* sequoiaTransaction)
        : SequoiaTransaction_(sequoiaTransaction)
    { }

    TSimpleTransactionReplicator& AddTransaction(const NRecords::TTransaction& transaction)
    {
        auto* subrequest = Action_.add_transactions();
        ToProto(subrequest->mutable_id(), transaction.Key.TransactionId);
        ToProto(subrequest->mutable_parent_id(), transaction.AncestorIds.empty()
            ? NullTransactionId
            : transaction.AncestorIds.back());
        subrequest->set_upload(false);

        const auto& attributes = transaction.Attributes;

        #define MAYBE_SET_ATTRIBUTE(attribute_name) \
            if (auto attributeValue = \
                    attributes->FindChildValue<TString>(#attribute_name)) \
            { \
                subrequest->set_##attribute_name(*attributeValue); \
            }

        MAYBE_SET_ATTRIBUTE(title)
        MAYBE_SET_ATTRIBUTE(operation_type)
        MAYBE_SET_ATTRIBUTE(operation_id)
        MAYBE_SET_ATTRIBUTE(operation_title)

        #undef MAYBE_SET_ATTRIBUTE

        TransactionIds_.push_back(transaction.Key.TransactionId);
        return *this;
    }

    TSimpleTransactionReplicator& AddCell(TCellTag cellTag)
    {
        CellTags_.push_back(cellTag);
        return *this;
    }

    TSimpleTransactionReplicator& AddCells(TRange<TCellTag> cellTags)
    {
        CellTags_.insert(CellTags_.end(), cellTags.begin(), cellTags.end());
        return *this;
    }

    void Run()
    {
        auto transactionActionData = MakeTransactionActionData(Action_);
        for (auto cellTag : CellTags_) {
            SequoiaTransaction_->AddTransactionAction(cellTag, transactionActionData);

            for (auto transactionId : TransactionIds_) {
                SequoiaTransaction_->WriteRow(NRecords::TTransactionReplica{
                    .Key = {.TransactionId = transactionId, .CellTag = cellTag},
                    .Dummy = 0,
                });
            }
        }
    }

private:
    ISequoiaTransaction* const SequoiaTransaction_;
    TCompactVector<TTransactionId, 1> TransactionIds_;
    TTransactionReplicationDestinationCellTagList CellTags_;
    NProto::TReqMaterializeCypressTransactionReplicas Action_;
};

//! Handles transaction replication in common case.
/*!
 *  Since different transactions may be in ancestor-descendant relationship
 *  transaction hierarchy is properly handled here in a non-trivial way:
 *  1. collect all ancestors, topologically sort them and remove duplicates;
 *  2. fetch ancestors' replicas to not replicate transaction to the same cell
 *     twice;
 *  3. materialize transaction on foreign cells via transaction actions;
 *  4. modify "transaction_replicas" Sequoia table.
 */
struct TTransactionReplicator
    : TRefCounted
{
public:
    TTransactionReplicator(
        ISequoiaTransactionPtr sequoiaTransaction,
        std::vector<std::optional<NRecords::TTransaction>> transactions,
        TTransactionReplicationDestinationCellTagList cellTags)
        : SequoiaTransaction_(std::move(sequoiaTransaction))
        , Invoker_(NRpc::TDispatcher::Get()->GetHeavyInvoker())
        , CellTags_(std::move(cellTags))
    {
        CollectAndTopologicallySortAllAncestors(std::move(transactions));
    }

    template <CInvocable<void(TRange<std::optional<NRecords::TTransaction>>)> F>
    void IterateOverInnermostTransactionGroupedByCoordinator(F&& callback)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(!InnermostTransactions_.empty());

        int currentGroupStart = 0;
        for (int i = 1; i < std::ssize(InnermostTransactions_); ++i) {
            const auto& previous = CellTagFromId(InnermostTransactions_[i - 1]->Key.TransactionId);
            const auto& current = CellTagFromId(InnermostTransactions_[i]->Key.TransactionId);
            if (previous != current) {
                callback(TRange(InnermostTransactions_).Slice(currentGroupStart, i));
                currentGroupStart = i;
            }
        }

        callback(TRange(InnermostTransactions_)
            .Slice(currentGroupStart, InnermostTransactions_.size()));
    }

    TFuture<void> Run()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        return FetchAncestorsAndReplicas()
            .ApplyUnique(BIND(
                &TTransactionReplicator::ReplicateTransactions,
                MakeStrong(this))
                .AsyncVia(Invoker_));
    }

private:
    const ISequoiaTransactionPtr SequoiaTransaction_;
    const IInvokerPtr Invoker_;
    const TTransactionReplicationDestinationCellTagList CellTags_;
    std::vector<std::optional<NRecords::TTransaction>> InnermostTransactions_;
    std::vector<TTransactionId> AncestorIds_;

    struct TFetchedInfo
    {
        // |nullopt| means that certain transaction is NOT presented on certain
        // master cell. Of course, it's simplier to use vector<bool> instead but
        // we want to avoid unnecessary allocations here.
        // Order is a bit complicated:
        // (cell1, ancestor1), (cell1, ancestor2), ...
        // (cell1, transaction1), (cell1, transaction2), ...
        // (cell2, ancestor1), ...
        std::vector<std::optional<NRecords::TTransactionReplica>> Replicas;
        std::vector<std::optional<NRecords::TTransaction>> Ancestors;

        // TODO(kvk1920): add method IsReplicatedToCell() and use it instead of
        // looking into #Replicas directly.
    };

    void ReplicateTransactions(TFetchedInfo&& fetchedInfo)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto totalTransactionCount = AncestorIds_.size() + InnermostTransactions_.size();

        auto cellCount = std::ssize(CellTags_);
        // See comment in |TFetchedInfo|.
        for (int cellIndex = 0; cellIndex < cellCount; ++cellIndex) {
            auto replicaPresence = TRange(fetchedInfo.Replicas).Slice(
                totalTransactionCount * cellIndex,
                totalTransactionCount * (cellIndex + 1));
            auto ancestorReplicaPresence = replicaPresence.Slice(0, AncestorIds_.size());
            auto transactionReplicaPresence = replicaPresence.Slice(
                AncestorIds_.size(),
                replicaPresence.size());
            ReplicateToCell(
                fetchedInfo.Ancestors,
                ancestorReplicaPresence,
                transactionReplicaPresence,
                CellTags_[cellIndex]);
        }
    }

    void ReplicateToCell(
        TRange<std::optional<NRecords::TTransaction>> ancestors,
        TRange<std::optional<NRecords::TTransactionReplica>> ancestorReplicas,
        TRange<std::optional<NRecords::TTransactionReplica>> transactionReplicas,
        TCellTag cellTag)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        TSimpleTransactionReplicator replicator(SequoiaTransaction_.Get());
        replicator.AddCell(cellTag);

        auto replicateTransactions = [&] (
            TRange<std::optional<NRecords::TTransaction>> transactions,
            TRange<std::optional<NRecords::TTransactionReplica>> replicas)
        {
            YT_ASSERT(transactions.size() == replicas.size());

            for (int i = 0; i < std::ssize(replicas); ++i) {
                if (!replicas[i]) {
                    // There is no such replica so replication is needed.
                    replicator.AddTransaction(*transactions[i]);
                }
            }
        };

        replicateTransactions(ancestors, ancestorReplicas);
        replicateTransactions(InnermostTransactions_, transactionReplicas);

        replicator.Run();
    }

    TFuture<TFetchedInfo> FetchAncestorsAndReplicas()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto ancestors = FetchAncestors();
        auto replicas = FetchReplicas();

        // Fast path.
        if (!ancestors) {
            return replicas.ApplyUnique(BIND(
                [] (std::vector<std::optional<NRecords::TTransactionReplica>>&& replicas) {
                    return TFetchedInfo{
                        .Replicas = std::move(replicas),
                    };
                }).AsyncVia(Invoker_));
        }

        return ancestors.ApplyUnique(BIND([
            replicas = std::move(replicas),
            this,
            this_ = MakeStrong(this)
        ] (std::vector<std::optional<NRecords::TTransaction>>&& ancestors) {
            ValidateAncestors(ancestors);

            return replicas.ApplyUnique(BIND([
                ancestors = std::move(ancestors)
            ] (std::vector<std::optional<NRecords::TTransactionReplica>>&& replicas) {
                return TFetchedInfo{
                    .Replicas = std::move(replicas),
                    .Ancestors = std::move(ancestors),
                };
            }));
        })
            .AsyncVia(Invoker_));
    }

    TFuture<std::vector<std::optional<NRecords::TTransaction>>> FetchAncestors()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        // Fast path.
        if (AncestorIds_.empty()) {
            return std::nullopt;
        }

        auto keys = ToTransactionKeys(AncestorIds_);

        return SequoiaTransaction_->LookupRows(keys);
    }

    TFuture<std::vector<std::optional<NRecords::TTransactionReplica>>> FetchReplicas()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto totalTransactionCount = AncestorIds_.size() + InnermostTransactions_.size();
        std::vector<NRecords::TTransactionReplicaKey> keys(
            totalTransactionCount * CellTags_.size());
        for (int cellTagIndex = 0; cellTagIndex < std::ssize(CellTags_); ++cellTagIndex) {
            auto cellTag = CellTags_[cellTagIndex];

            auto ancestorsOffset = totalTransactionCount * cellTagIndex;
            std::transform(
                AncestorIds_.begin(),
                AncestorIds_.end(),
                keys.begin() + ancestorsOffset,
                [=] (TTransactionId id) {
                    return NRecords::TTransactionReplicaKey{
                        .TransactionId = id,
                        .CellTag = cellTag,
                    };
                });

            auto transactionsOffset = ancestorsOffset + AncestorIds_.size();
            std::transform(
                InnermostTransactions_.begin(),
                InnermostTransactions_.end(),
                keys.begin() + transactionsOffset,
                [=] (const std::optional<NRecords::TTransaction>& record) {
                    return NRecords::TTransactionReplicaKey{
                        .TransactionId = record->Key.TransactionId,
                        .CellTag = cellTag,
                    };
                });
        }

        return SequoiaTransaction_->LookupRows(keys);
    }

    void ValidateAncestors(const std::vector<std::optional<NRecords::TTransaction>>& records)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        ValidateAllTransactionsExist(records);
        ValidateTransactionAncestors(records);
    }

    void CollectAndTopologicallySortAllAncestors(
        std::vector<std::optional<NRecords::TTransaction>> transactions)
    {
        // We need to process every ancestor only once so we need to collect and
        // remove duplicates.
        THashSet<TTransactionId> allAncestors;
        for (const auto& transaction : transactions) {
            allAncestors.insert(
                transaction->AncestorIds.begin(),
                transaction->AncestorIds.end());
        }

        // We don't have to send replication requests for ancestors since
        // innermost transactions' replication already causes replication of
        // ancestors.
        transactions.erase(
            std::remove_if(
                transactions.begin(),
                transactions.end(),
                [&] (const std::optional<NRecords::TTransaction>& record) {
                    return allAncestors.contains(record->Key.TransactionId);
                }),
            transactions.end());
        SortBy(transactions, [] (const std::optional<NRecords::TTransaction>& record) {
            return CellTagFromId(record->Key.TransactionId);
        });

        // TODO(kvk1920): optimize.
        // #transactions may contain some ancestors, but we throw them away and
        // fetch again. We could avoid some lookups here. (Of course, it is
        // unlikely to be a bottleneck since lookups are done in parallel.
        // Rather, it's all about lookup latency).

        // Since transactions are instantiated in the order they are presented
        // here we have to sort them topologically: every ancestor of
        // transaction "T" must take a place somewhere before transaction "T".
        // This is the reason for this instead of just
        // |std::vector(allAncestors.begin(), allAncestors.end())|.
        AncestorIds_.reserve(allAncestors.size());
        for (const auto& record : transactions) {
            // NB: ancestor_ids in "transactions" Sequoia table are always
            // topologically sorted.
            for (auto ancestorId : record->AncestorIds) {
                if (auto it = allAncestors.find(ancestorId); it != allAncestors.end()) {
                    allAncestors.erase(it);
                    AncestorIds_.push_back(ancestorId);
                }
            }
        }
        InnermostTransactions_ = std::move(transactions);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Modifies both master persistent state and Sequoia tables.
/*!
 *  NB: All actions are executed via RPC heavy invoker.
 */
template <class TResult>
class TSequoiaMutation
    : public TRefCounted
{
public:
    TFuture<TResult> Apply()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND(&TSequoiaMutation::DoApply, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
    }

protected:
    TBootstrap* const Bootstrap_;
    const IInvokerPtr Invoker_;

    // Initialized once per class lifetime.
    ISequoiaTransactionPtr SequoiaTransaction_;

    TSequoiaMutation(
        TBootstrap* bootstrap,
        TStringBuf description)
        : Bootstrap_(bootstrap)
        , Invoker_(NRpc::TDispatcher::Get()->GetHeavyInvoker())
        , Description_(description)
    {
        VERIFY_THREAD_AFFINITY_ANY();
    }

    TFuture<void> CommitSequoiaTransaction(TCellId coordinatorCellId)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        return SequoiaTransaction_->Commit({
            .CoordinatorCellId = coordinatorCellId,
            .CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Late,
        });
    }

    virtual TFuture<TResult> ApplyAndCommitSequoiaTransaction() = 0;

private:
    const TStringBuf Description_;

    TFuture<TResult> DoApply()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Bootstrap_
            ->GetSequoiaClient()
            ->StartTransaction()
            .ApplyUnique(
                BIND(&TSequoiaMutation::OnSequoiaTransactionStarted, MakeStrong(this))
                    .AsyncVia(Invoker_));
    }

    TFuture<TResult> OnSequoiaTransactionStarted(ISequoiaTransactionPtr&& sequoiaTransaction)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(!SequoiaTransaction_);

        SequoiaTransaction_ = std::move(sequoiaTransaction);

        return ApplyAndCommitSequoiaTransaction()
            .Apply(BIND(&TSequoiaMutation::ProcessResult, MakeStrong(this))
                .AsyncVia(Invoker_));
    }

    TResult ProcessResult(const TErrorOr<TResult>& result)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (result.IsOK()) {
            if constexpr (std::is_void_v<TResult>) {
                return;
            } else {
                return result.Value();
            }
        }

        if (auto error = result.FindMatching(NSequoiaClient::EErrorCode::SequoiaTableCorrupted)) {
            // NB: consider disabling Cypress tx mirroring by setting
            // //sys/@config/sequoia_manager/enable_cypress_transactions_in_sequoia to false.
            // Ensure that you actually know what are you doing.
            YT_LOG_ALERT(
                *error,
                "Failed to %v Cypress transaction on Sequoia",
                Description_);
        }

        if (IsRetriableSequoiaError(result)) {
            THROW_ERROR_EXCEPTION(
                NSequoiaClient::EErrorCode::SequoiaRetriableError,
                "Sequoia retriable error")
                << std::move(result);
        }

        THROW_ERROR result;
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Starts Cypress transaction mirrored to Sequoia tables.
/*!
 *   1. Generate new transaction id;
 *   2. If there is no parent transaction then go to step 6;
 *   3. Lock parent transaction in "transactions" Sequoia table;
 *   4. Fetch parent's ancestors;
 *   5. Write (ancestor_id, transaction_id) to table "transaction_descendants" for
 *      every ancestor;
 *   6. Write (transaction_id, ancestor_ids) to table "transactions";
 *   7. Write (prerequisite_id, transaction_id) to table "dependent_transactions";
 *   8. Execute StartCypressTransaction tx action on coordinator;
 *   9. Execute StartForeignTransaction tx action on every cell which this
 *      transaction should be replicated to;
 *  10. Reply with transaction id generated in step 1.
 */
class TStartCypressTransaction
    : public TSequoiaMutation<TSharedRefArray>
{
public:
    TStartCypressTransaction(
        TBootstrap* bootstrap,
        NCypressTransactionClient::NProto::TReqStartTransaction request,
        NRpc::TAuthenticationIdentity authenticationIdentity)
        : TSequoiaMutation(bootstrap, "start")
        , ParentId_(FromProto<TTransactionId>(request.parent_id()))
        , ReplicateToCellTags_(BuildReplicateToCellTags(
            Bootstrap_->GetCellTag(),
            FromProto<TCellTagList>(request.replicate_to_cell_tags())))
        , PrerequisiteTransactionIds_(MakeSortedAndUnique(
            FromProto<std::vector<TTransactionId>>(request.prerequisite_transaction_ids())))
        , Request_(BuildStartCypressTransactionRequest(
            std::move(request),
            std::move(authenticationIdentity)))
    { }

protected:
    TFuture<TSharedRefArray> ApplyAndCommitSequoiaTransaction() override
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);
        YT_VERIFY(SequoiaTransaction_);

        auto transactionId = SequoiaTransaction_->GenerateObjectId(
            ParentId_ ? EObjectType::NestedTransaction : EObjectType::Transaction,
            Bootstrap_->GetCellTag());
        ToProto(Request_.mutable_hint_id(), transactionId);

        auto createResponseMessage = BIND([transactionId] {
            NProto::TRspStartCypressTransaction rspProto;
            ToProto(rspProto.mutable_id(), transactionId);
            return NRpc::CreateResponseMessage(rspProto);
        }).AsyncVia(Invoker_);

        // Fast path.
        if (!ParentId_ && PrerequisiteTransactionIds_.empty()) {
            auto asyncResult = ModifyTablesAndRegisterActions();
            // Fast path is synchronous.
            YT_VERIFY(asyncResult.IsSet());
            return CommitSequoiaTransaction(Bootstrap_->GetCellId())
                .Apply(createResponseMessage);
        }

        return HandlePrerequisiteTransactions()
            .Apply(BIND(
                &TStartCypressTransaction::LockParentAndCollectAncestors,
                MakeStrong(this))
                    .AsyncVia(Invoker_))
            .ApplyUnique(BIND(
                &TStartCypressTransaction::ModifyTablesAndRegisterActions,
                MakeStrong(this))
                    .AsyncVia(Invoker_))
            .Apply(BIND(
                &TStartCypressTransaction::CommitSequoiaTransaction,
                MakeStrong(this),
                Bootstrap_->GetCellId())
                    .AsyncVia(Invoker_))
            .Apply(createResponseMessage);
    }

private:
    const TTransactionId ParentId_;
    const TCellTagList ReplicateToCellTags_;
    const std::vector<TTransactionId> PrerequisiteTransactionIds_;

    // NB: transaction ID is set after Sequoia tx is started.
    NProto::TReqStartCypressTransaction Request_;

    static TCellTagList BuildReplicateToCellTags(TCellTag thisCellTag, TCellTagList cellTags)
    {
        cellTags.erase(std::remove(cellTags.begin(), cellTags.end(), thisCellTag), cellTags.end());
        Sort(cellTags);
        return cellTags;
    }

    TFuture<void> ModifyTablesAndRegisterActions(
        std::vector<TTransactionId>&& ancestorIds = {}) const
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto transactionId = FromProto<TTransactionId>(Request_.hint_id());

        for (auto ancestorId : ancestorIds) {
            SequoiaTransaction_->WriteRow(NRecords::TTransactionDescendant{
                .Key = {.TransactionId = ancestorId, .DescendantId = transactionId},
                .Dummy = 0,
            });
        }

        auto attributes = FromProto(Request_.attributes());
        auto attributeKeys = attributes->ListKeys();
        // Only those attributes which are necessary for tx replication are
        // stored in Sequoia table.
        for (const auto& attributeName : attributeKeys) {
            if (attributeName != "operation_type" &&
                attributeName != "operation_id" &&
                attributeName != "operation_title")
            {
                attributes->Remove(attributeName);
            }
        }

        if (Request_.has_title()) {
            attributes->Set("title", Request_.title());
        }

        auto createdTransaction = NRecords::TTransaction{
            .Key = {.TransactionId = transactionId},
            .AncestorIds = std::move(ancestorIds),
            .Attributes = attributes->ToMap(),
            .PrerequisiteTransactionIds = PrerequisiteTransactionIds_,
        };

        SequoiaTransaction_->WriteRow(createdTransaction);

        SequoiaTransaction_->AddTransactionAction(
            Bootstrap_->GetCellTag(),
            MakeTransactionActionData(Request_));

        // NB: all of these transactions should be already locked.
        for (auto prerequisiteTransactionId : PrerequisiteTransactionIds_) {
            if (!IsSequoiaId(prerequisiteTransactionId)) {
                // One may use system transaction as prerequisite. Since system
                // transactions are not mirrored we shouldn't put any info about
                // them into Sequoia tables.

                // NB: abort of such dependent transactions will be replicated
                // via Hive.
                continue;
            }

            SequoiaTransaction_->WriteRow(NRecords::TDependentTransaction{
                .Key = {
                    .TransactionId = prerequisiteTransactionId,
                    .DependentTransactionId = transactionId,
                },
                .Dummy = 0,
            });
        }

        // Fast path.
        if (ReplicateToCellTags_.empty()) {
            return VoidFuture;
        }

        // Another fast path.
        if (!ParentId_) {
            // Transaction hierarchy is trivial and coordinator is already knows
            // about replicas so we can use TSimpleTransactionReplicator here.
            TSimpleTransactionReplicator(SequoiaTransaction_.Get())
                .AddTransaction(std::move(createdTransaction))
                .AddCells(ReplicateToCellTags_)
                .Run();
            return VoidFuture;
        }

        return New<TTransactionReplicator>(
            SequoiaTransaction_,
            std::vector{std::optional(std::move(createdTransaction))},
            ReplicateToCellTags_)
            ->Run();
    }

    TFuture<std::vector<TTransactionId>> CheckParentAndGetParentAncestors(
        std::vector<std::optional<NRecords::TTransaction>>&& responses) const
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);
        YT_VERIFY(responses.size() == 1);

        if (!responses.front()) {
            ThrowNoSuchTransaction(ParentId_);
        }

        ValidateTransactionAncestors(*responses.front());

        auto ancestors = std::move(responses.front()->AncestorIds);
        ancestors.push_back(responses.front()->Key.TransactionId);

        return MakeFuture(ancestors);
    }

    TFuture<std::vector<TTransactionId>> LockParentAndCollectAncestors() const
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (!ParentId_) {
            return MakeFuture<std::vector<TTransactionId>>({});
        }

        // Shared read lock prevents concurrent parent trasaction
        // commit or abort but still allows to start another nested transaction
        // concurrenctly.
        SequoiaTransaction_->LockRow(
            NRecords::TTransactionKey{.TransactionId = ParentId_},
            ELockType::SharedStrong);

        const auto& idMapping = NRecords::TTransactionDescriptor::Get()->GetIdMapping();
        return SequoiaTransaction_->LookupRows<NRecords::TTransactionKey>(
            {{.TransactionId = ParentId_}},
            {*idMapping.TransactionId, *idMapping.AncestorIds})
            .ApplyUnique(BIND(
                &TStartCypressTransaction::CheckParentAndGetParentAncestors,
                MakeStrong(this))
                    .AsyncVia(Invoker_));
    }

    void ValidateAndLockPrerequisiteTransactions(
        std::vector<std::optional<NRecords::TTransaction>>&& records)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(PrerequisiteTransactionIds_.size() == records.size());

        for (int i = 0; i < std::ssize(records); ++i) {
            if (!records[i]) {
                ThrowPrerequisiteCheckFailedNoSuchTransaction(PrerequisiteTransactionIds_[i]);
            }
        }

        ValidateTransactionAncestors(records);

        for (const auto& record : records) {
            SequoiaTransaction_->LockRow(record->Key, ELockType::SharedStrong);
        }
    }

    TFuture<void> HandlePrerequisiteTransactions()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (PrerequisiteTransactionIds_.empty()) {
            return VoidFuture;
        }

        return SequoiaTransaction_->LookupRows(ToTransactionKeys(PrerequisiteTransactionIds_))
            .ApplyUnique(BIND(
                &TStartCypressTransaction::ValidateAndLockPrerequisiteTransactions,
                MakeStrong(this))
                    .AsyncVia(Invoker_));
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Collects all dependent transactions transitively and finds topmost unique
//! dependent transactions.
/*!
 *  This class is used to implement transaction finishing: when transaction is
 *  committed or aborted, all its dependent (and nested) transactions are aborted
 *  too. To achive this we have to collect all dependent transactions and find
 *  topmost ones: it's sufficient to abort only subtree's root because it leads
 *  to abortion of all subtree.
 *
 *  "dependent_transactions" Sequoia table does not contains transitive closure
 *  of all dependent transactions (in opposite to "trasaction_descendants")
 *  because there is no any sane bound for number of dependent transactions.
 *  So the collection of all dependent transactions is a bit non-trivial:
 *
 *  collectedTransactions -- set of fetched transactions;
 *  currenTTransaction -- set of transaction IDs with unchecked dependent
 *      transactions and descendants;
 *
 *  collectedTransactions := {targetTransaction}
 *  currenTTransaction := {targetTransaction.Id}
 *  while not currenTTransaction.empty():
 *      nexTTransaction :=
 *          select descendant_id
 *              from transaction_descendants
 *              where transaction_id in currenTTransaction
 *          +
 *          select dependent_transactions_id
 *              from dependent_transactions
 *              where transaction_id in currenTTransaction
 *
 *      currenTTransaction := {}
 *      for transaction in nexTTransaction:
 *          if transaction not in collectedTransactions:
 *              currenTTransaction.add(transaction.Id)
 *              collectedTransactions.add(transaction)
 *  return collectedTransactions
 */
class TDependenTTransactionCollector
    : public TRefCounted
{
public:
    TDependenTTransactionCollector(
        ISequoiaTransactionPtr sequoiaTransaction,
        NRecords::TTransaction targetTransaction)
        : SequoiaTransaction_(std::move(sequoiaTransaction))
        , TargetTransaction_(std::move(targetTransaction))
        , Invoker_(NRpc::TDispatcher::Get()->GetHeavyInvoker())
    { }

    struct TResult
    {
        // Contains topmost dependent transactions.
        std::vector<TTransactionId> DependenTTransactionubtreeRoots;
        THashMap<TTransactionId, NRecords::TTransaction> Transactions;

        // NB: despite we fetch records from "dependent_transactions" and
        // "transaction_descendants" we don't return them since they are not
        // required to handle transaction finish: record from "transactions"
        // table contains "prerequisite_transaction_ids" and "ancestor_ids" and
        // it is enough to clean up "dependent_transactions" and
        // "transaction_descendants".
    };

    TFuture<TResult> Run()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        CollectedTransactions_[TargetTransaction_.Key.TransactionId] = TargetTransaction_;
        CurrenTTransaction_.push_back(TargetTransaction_.Key.TransactionId);

        return CollectMoreTransactions()
            .Apply(BIND(&TDependenTTransactionCollector::MakeResult, MakeStrong(this))
                .AsyncVia(Invoker_));
    }

private:
    const ISequoiaTransactionPtr SequoiaTransaction_;
    const NRecords::TTransaction TargetTransaction_;
    const IInvokerPtr Invoker_;

    // This state is shared between different callback invokations.
    THashMap<TTransactionId, NRecords::TTransaction> CollectedTransactions_;
    std::vector<TTransactionId> CurrenTTransaction_;

    TResult MakeResult() const
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        std::vector<TTransactionId> roots;

        for (const auto& [transactionId, record] : CollectedTransactions_) {
            if (transactionId == TargetTransaction_.Key.TransactionId) {
                continue;
            }

            // NB: checking transaction's parent is sufficient: if some ancestor
            // "A" of transaction "T" is collected then all its descendants are
            // collected too; so one of these descendants is parent of "T".
            if (record.AncestorIds.empty() ||
                !CollectedTransactions_.contains(record.AncestorIds.back()))
            {
                roots.push_back(transactionId);
            }
        }

        return {
            .DependenTTransactionubtreeRoots = std::move(roots),
            .Transactions = CollectedTransactions_,
        };
    }

    TFuture<void> CollectMoreTransactions()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (CurrenTTransaction_.empty()) {
            return VoidFuture;
        }

        return FetchNexTTransaction()
            .ApplyUnique(BIND(
                &TDependenTTransactionCollector::ProcessNexTTransaction,
                MakeStrong(this))
                .AsyncVia(Invoker_))
            .Apply(
                BIND(&TDependenTTransactionCollector::CollectMoreTransactions, MakeStrong(this))
                    .AsyncVia(Invoker_));
    }

    void ProcessNexTTransaction(std::vector<std::optional<NRecords::TTransaction>>&& records)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        ValidateAllTransactionsExist(records);
        ValidateTransactionAncestors(records);

        CurrenTTransaction_.clear();
        CurrenTTransaction_.reserve(records.size());
        for (auto& record : records) {
            auto transactionId = record->Key.TransactionId;
            if (CollectedTransactions_.emplace(transactionId, std::move(*record)).second) {
                CurrenTTransaction_.push_back(transactionId);
            }
        }
    }

    TFuture<std::vector<std::optional<NRecords::TTransaction>>> FetchNexTTransaction() const
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        auto condition = BuildSelectByTransactionIds(CurrenTTransaction_, [] (auto transactionId) {
            return transactionId;
        });
        auto descendentTransaction = SequoiaTransaction_
            ->SelectRows<NRecords::TTransactionDescendant>({condition});

        auto dependentTransaction = SequoiaTransaction_
            ->SelectRows<NRecords::TDependentTransaction>({condition});

        return AllSucceeded(
            std::vector{descendentTransaction.AsVoid(), dependentTransaction.AsVoid()})
                .Apply(BIND([
                    this,
                    this_ = MakeStrong(this),
                    descendentTransactionFuture = descendentTransaction,
                    dependentTransactionFuture = dependentTransaction
                ] {
                    VERIFY_INVOKER_AFFINITY(Invoker_);
                    YT_ASSERT(descendentTransactionFuture.IsSet());
                    YT_ASSERT(dependentTransactionFuture.IsSet());

                    // NB: AllSucceeded() guarantees that all futures contain values.
                    const auto& descendentTransaction = descendentTransactionFuture.Get().Value();
                    const auto& dependentTransaction = dependentTransactionFuture.Get().Value();

                    if (descendentTransaction.empty() && dependentTransaction.empty()) {
                        return MakeFuture(std::vector<std::optional<NRecords::TTransaction>>{});
                    }

                    std::vector<NRecords::TTransactionKey> keys;
                    keys.reserve(descendentTransaction.size() + dependentTransaction.size());

                    for (const auto& record : dependentTransaction) {
                        auto id = record.Key.DependentTransactionId;
                        if (!CollectedTransactions_.contains(id)) {
                            keys.push_back(NRecords::TTransactionKey{.TransactionId = id});
                        }
                    }

                    for (const auto& record : descendentTransaction) {
                        auto id = record.Key.DescendantId;
                        if (!CollectedTransactions_.contains(id)) {
                            keys.push_back(NRecords::TTransactionKey{.TransactionId = id});
                        }
                    }

                    return SequoiaTransaction_->LookupRows(keys);
                })
                    .AsyncVia(Invoker_));
    }
};

//! This class is responsible for finishing transactions: commit and abort are
//! handled in a similar way.
/*!
 *  When transaction is finished (because of commit or abort) every descendant
 *  and dependent transaction has to be aborted. On transaction coordinator it's
 *  handled in commit/abort mutation but we still need to clean Sequoia tables
 *  and replicate abort mutations to all participants.
 *  1. Fetch target transaction (and validate it);
 *  2. Fetch all descendant and dependent transactions (transitively);
 *  3. Find all subtrees' roots (target tx + all dependent txs);
 *  4. For each transaction to abort:
 *     4.1. Execute abort tx action on transaction coordinator;
 *     4.2. Execute abort tx action on every participant;
 *     4.3. Remove all replicas from "transaction_replicas" table;
 *     4.4. Remove (prerequisite_transaction_id, transaction_id) from
 *          "dependent_transactions" table;
 *     4.5. Remove (ancestor_id, transaction_id) for every its ancestor from
 *          "transaction_descendants" table;
 *     4.6. Remove transaction from "tansactions" table.
 */
class TFinishCypressTransaction
    : public TSequoiaMutation<TSharedRefArray>
{
protected:
    const TTransactionId TransactionId_;
    const NRpc::TAuthenticationIdentity AuthenticationIdentity_;

    TFinishCypressTransaction(
        TBootstrap* bootstrap,
        TStringBuf description,
        TTransactionId transactionId,
        NRpc::TAuthenticationIdentity authenticationIdentity)
        : TSequoiaMutation(bootstrap, description)
        , TransactionId_(transactionId)
        , AuthenticationIdentity_(std::move(authenticationIdentity))
    { }

    TFuture<TSharedRefArray> ApplyAndCommitSequoiaTransaction() final
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        return FetchTargetTransaction()
            .ApplyUnique(BIND(
                &TFinishCypressTransaction::CollectDependentAndNestedTransactionsAndFinishThem,
                MakeStrong(this))
                    .AsyncVia(Invoker_))
            .Apply(BIND(
                &TFinishCypressTransaction::CommitSequoiaTransaction,
                MakeStrong(this),
                Bootstrap_->GetCellId())
                    .AsyncVia(Invoker_))
            .Apply(BIND(&TFinishCypressTransaction::CreateResponseMessage, MakeStrong(this))
                .AsyncVia(Invoker_));
    }

    // Returns |false| if transaction shouldn't be processed (e.g. force abort
    // of non-existent transaction should not be treated as an error).
    virtual bool CheckTargetTransaction(
        const std::optional<NRecords::TTransaction>& record) = 0;

    virtual TSharedRefArray CreateResponseMessage() = 0;

    // Register transaction actions for Sequoia transaction.
    virtual void FinishTargetTransactionOnMaster(
        TRange<NRecords::TTransactionReplica> replicas) = 0;

    void AbortTransactionOnParticipants(TRange<NRecords::TTransactionReplica> replicas)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        if (replicas.empty()) {
            // This transaction is not replicated to anywhere.
            return;
        }

        NProto::TReqAbortTransaction request;
        ToProto(request.mutable_transaction_id(), replicas.Front().Key.TransactionId);
        request.set_force(true);
        auto transactionAction = MakeTransactionActionData(request);
        for (const auto& replica : replicas) {
            SequoiaTransaction_->AddTransactionAction(replica.Key.CellTag, transactionAction);
        }
    }

private:
    TFuture<void> DoFinishTransactions(
        TDependenTTransactionCollector::TResult&& transactionInfos)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        return FetchReplicas(transactionInfos.Transactions)
            .ApplyUnique(BIND(
                &TFinishCypressTransaction::OnReplicasFetched,
                MakeStrong(this),
                std::move(transactionInfos))
                    .AsyncVia(Invoker_));
    }

    void OnReplicasFetched(
        TDependenTTransactionCollector::TResult transactionsInfo,
        std::vector<NRecords::TTransactionReplica>&& replicas)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        // ORDER BY expression cannot help us here since IDs are stored as
        // strings and string and ID orders are different.
        SortBy(replicas, [] (const auto& record) {
            return record.Key.TransactionId;
        });

        FinishTargetTransactionOnMaster(FindReplicas(replicas, TransactionId_));

        // On transaction coordinator dependent transaction aborts are caused by
        // target transaction finishing. However, this abort has to be
        // replicated to other participants.
        for (auto transactionId : transactionsInfo.DependenTTransactionubtreeRoots) {
            AbortTransactionOnParticipants(FindReplicas(replicas, transactionId));
        }

        // Remove transactions from Sequoia tables.

        // TODO(kvk1920): remove branches.

        // "transaction_replicas"
        for (const auto& replica : replicas) {
            SequoiaTransaction_->DeleteRow(replica.Key);
        }

        for (const auto& [transactionId, transactionInfo] : transactionsInfo.Transactions) {
            // "dependent_transactions"
            for (auto prerequisiteTransactionId : transactionInfo.PrerequisiteTransactionIds) {
                SequoiaTransaction_->DeleteRow(NRecords::TDependentTransactionKey{
                    .TransactionId = prerequisiteTransactionId,
                    .DependentTransactionId = transactionId,
                });
            }
            // "transaction_descendants"
            for (auto ancestorId : transactionInfo.AncestorIds) {
                SequoiaTransaction_->DeleteRow(NRecords::TTransactionDescendantKey{
                    .TransactionId = ancestorId,
                    .DescendantId = transactionId,
                });
            }
            // "transactions"
            SequoiaTransaction_->DeleteRow(transactionInfo.Key);
        }
    }

    static TRange<NRecords::TTransactionReplica> FindReplicas(
        const std::vector<NRecords::TTransactionReplica>& replicas,
        TTransactionId transactionId)
    {
        static constexpr auto idFromReplica = [] (const NRecords::TTransactionReplica& record) {
            return record.Key.TransactionId;
        };

        auto begin = LowerBoundBy(replicas.begin(), replicas.end(), transactionId, idFromReplica);
        auto end = UpperBoundBy(replicas.begin(), replicas.end(), transactionId, idFromReplica);

        return TRange(replicas).Slice(begin - replicas.begin(), end - replicas.begin());
    }

    TFuture<std::vector<std::optional<NRecords::TTransaction>>> FetchTargetTransaction()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return SequoiaTransaction_->LookupRows<NRecords::TTransactionKey>(
            {{.TransactionId = TransactionId_}});
    }

    TFuture<void> CollectDependentAndNestedTransactionsAndFinishThem(
        std::vector<std::optional<NRecords::TTransaction>>&& target)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        YT_VERIFY(target.size() == 1);

        if (!CheckTargetTransaction(target.front())) {
            return VoidFuture;
        }

        // Case of absence target transaction is handled in
        // CheckTargetTransaction().
        YT_VERIFY(target.front());

        ValidateTransactionAncestors(*target.front());

        // TODO(kvk1920): target transaction branches should be merged here.

        return New<TDependenTTransactionCollector>(
            SequoiaTransaction_,
            std::move(*target.front()))
                ->Run()
                .ApplyUnique(
                    BIND(&TFinishCypressTransaction::DoFinishTransactions, MakeStrong(this))
                        .AsyncVia(Invoker_));
    }

    TFuture<std::vector<NRecords::TTransactionReplica>> FetchReplicas(
        const THashMap<TTransactionId, NRecords::TTransaction>& transactions)
    {
        return SequoiaTransaction_->SelectRows<NRecords::TTransactionReplica>(
            BuildSelectByTransactionIds(transactions, [] (const auto& pair) {
                return pair.first;
            }));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAbortCypressTransaction
    : public TFinishCypressTransaction
{
public:
    TAbortCypressTransaction(
        TBootstrap* bootstrap,
        const NCypressTransactionClient::NProto::TReqAbortTransaction& request,
        NRpc::TAuthenticationIdentity authenticationIdentity)
        : TFinishCypressTransaction(
            bootstrap,
            "abort",
            FromProto<TTransactionId>(request.transaction_id()),
            std::move(authenticationIdentity))
        , Force_(request.force())
    { }

    TAbortCypressTransaction(
        TBootstrap* bootstrap,
        TTransactionId transactionId,
        bool force,
        NRpc::TAuthenticationIdentity authenticationIdentity)
        : TFinishCypressTransaction(
            bootstrap,
            "abort expired",
            transactionId,
            std::move(authenticationIdentity))
        , Force_(force)
    { }

protected:
    bool CheckTargetTransaction(const std::optional<NRecords::TTransaction>& record) override
    {
        if (record) {
            return true;
        }

        if (Force_) {
            return false;
        }

        ThrowNoSuchTransaction(TransactionId_);
    }

    TSharedRefArray CreateResponseMessage() override
    {
        return NRpc::CreateResponseMessage(
            NCypressTransactionClient::NProto::TRspAbortTransaction{});
    }

    void FinishTargetTransactionOnMaster(TRange<NRecords::TTransactionReplica> replicas) override
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        NProto::TReqAbortCypressTransaction req;
        ToProto(req.mutable_transaction_id(), TransactionId_);
        req.set_replicate_via_hive(false);
        req.set_force(Force_);
        NRpc::WriteAuthenticationIdentityToProto(&req, AuthenticationIdentity_);
        SequoiaTransaction_->AddTransactionAction(
            Bootstrap_->GetCellTag(),
            MakeTransactionActionData(req));

        AbortTransactionOnParticipants(replicas);
    }

private:
    const bool Force_;
};

////////////////////////////////////////////////////////////////////////////////

class TCommitCypressTransaction
    : public TFinishCypressTransaction
{
public:
    TCommitCypressTransaction(
        TBootstrap* bootstrap,
        TTransactionId transactionId,
        std::vector<TTransactionId> prerequisiteTransactionIds,
        TTimestamp commitTimestamp,
        NRpc::TAuthenticationIdentity authenticationIdentity)
        : TFinishCypressTransaction(
            bootstrap,
            "commit",
            transactionId,
            std::move(authenticationIdentity))
        , CommitTimestamp_(commitTimestamp)
        , PrerequisiteTransactionIds_(std::move(prerequisiteTransactionIds))
    { }

protected:
    bool CheckTargetTransaction(const std::optional<NRecords::TTransaction>& record) override
    {
        if (record) {
            return true;
        }

        ThrowNoSuchTransaction(TransactionId_);
    }

    TSharedRefArray CreateResponseMessage() override
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        NCypressTransactionClient::NProto::TRspCommitTransaction rsp;
        NHiveClient::TTimestampMap timestampMap;
        timestampMap.Timestamps.emplace_back(Bootstrap_->GetPrimaryCellTag(), CommitTimestamp_);
        ToProto(rsp.mutable_commit_timestamps(), timestampMap);
        return NRpc::CreateResponseMessage(rsp);
    }

    void FinishTargetTransactionOnMaster(
        TRange<NRecords::TTransactionReplica> replicas) override
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        SequoiaTransaction_->AddTransactionAction(
            Bootstrap_->GetCellTag(),
            MakeTransactionActionData(BuildCommitCypressTransactionRequest(
                TransactionId_,
                CommitTimestamp_,
                PrerequisiteTransactionIds_,
                AuthenticationIdentity_)));

        CommitTransactionOnParticipants(replicas);
    }

private:
    const TTimestamp CommitTimestamp_;
    const std::vector<TTransactionId> PrerequisiteTransactionIds_;

    void CommitTransactionOnParticipants(TRange<NRecords::TTransactionReplica> replicas)
    {
        if (!replicas.empty()) {
            NProto::TReqCommitTransaction req;
            ToProto(req.mutable_transaction_id(), TransactionId_);
            auto transactionActionData = MakeTransactionActionData(req);

            for (const auto& replica : replicas) {
                SequoiaTransaction_->AddTransactionAction(
                    replica.Key.CellTag,
                    transactionActionData);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Replicates given transactions from given cell to current cell. Note that
//! non-existent transaction is considered replicated to everywhere.
/*!
 *  For every request transaction which is not replicated to current cell:
 *  1. Lock row in table "transactions";
 *  2. Modify "transaction_replicas" table;
 *  3. Modify Tx coordinator's state;
 *  4. Modify current cell's state.
 */
class TReplicateCypressTransactions
    : public TSequoiaMutation<void>
{
    using TThis = TReplicateCypressTransactions;

protected:
    TReplicateCypressTransactions(TBootstrap* bootstrap, TRange<TTransactionId> transactionIds)
        : TSequoiaMutation(bootstrap, "replicate Cypress")
        , TransactionIds_(FilterTransactionIds(transactionIds, bootstrap->GetCellTag()))
    {
        VERIFY_THREAD_AFFINITY_ANY();
    }

    TFuture<void> ApplyAndCommitSequoiaTransaction() override
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        return SequoiaTransaction_->LookupRows(ToTransactionKeys(TransactionIds_))
            .ApplyUnique(BIND(&TThis::ReplicateTransactions, MakeStrong(this))
                .AsyncVia(Invoker_))
            .Apply(
                BIND(&TThis::CommitSequoiaTransaction, MakeStrong(this), Bootstrap_->GetCellId())
                    .AsyncVia(Invoker_));
    }

private:
    const std::vector<TTransactionId> TransactionIds_;

    static std::vector<TTransactionId> FilterTransactionIds(
        TRange<TTransactionId> transactionIds,
        TCellTag thisCellTag)
    {
        std::vector<TTransactionId> filteredTransactionIds(
            transactionIds.begin(),
            transactionIds.end());

        // Nobody should try to replicate tx to its native cell.
        filteredTransactionIds.erase(
            std::remove_if(
                filteredTransactionIds.begin(),
                filteredTransactionIds.end(),
                [=] (TTransactionId id) {
                    return CellTagFromId(id) == thisCellTag;
                }),
            filteredTransactionIds.end());
        return filteredTransactionIds;
    }

    TFuture<void> ReplicateTransactions(
        std::vector<std::optional<NRecords::TTransaction>>&& transactions)
    {
        // NB: "no such transaction" shouldn't be thrown here. Instead we make
        // it look like everything is replicated and request under transaction
        // will try to find transaction and get "no such transaction" error.
        transactions.erase(
            std::remove(transactions.begin(), transactions.end(), std::nullopt),
            transactions.end());

        ValidateTransactionAncestors(transactions);

        if (transactions.empty()) {
            return VoidFuture;
        }

        // |TTransactionReplicator| handles transactions hierarchy to allow us
        // to avoid replicating the same tx twice.
        auto replicator = New<TTransactionReplicator>(
            SequoiaTransaction_,
            std::move(transactions),
            TTransactionReplicationDestinationCellTagList{Bootstrap_->GetCellTag()});

        // NB: replication of transaction T with ancestors (P1, P2, ...) causes
        // replication of these ancestors too. So we don't need to send
        // replication requests for (P1, P2, ...).
        replicator->IterateOverInnermostTransactionGroupedByCoordinator(
            [&] (TRange<std::optional<NRecords::TTransaction>> group) {
                YT_VERIFY(!group.empty());

                auto coordinatorCellTag = CellTagFromId(group.Front()->Key.TransactionId);
                NProto::TReqMarkCypressTransactionsReplicatedToCells action;
                action.add_destination_cell_tags(ToProto<int>(Bootstrap_->GetCellTag()));
                action.mutable_transaction_ids()->Reserve(group.size());

                for (const auto& transaction : group) {
                    // To prevent concurrent commit/abort.
                    SequoiaTransaction_->LockRow(transaction->Key, ELockType::SharedStrong);

                    ToProto(action.add_transaction_ids(), transaction->Key.TransactionId);
                }

                SequoiaTransaction_->AddTransactionAction(
                    coordinatorCellTag,
                    MakeTransactionActionData(action));
            });

        return replicator->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void StartCypressTransactionInSequoiaAndReply(
    TBootstrap* bootstrap,
    const ITransactionManager::TCtxStartCypressTransactionPtr& context)
{
    context->ReplyFrom(New<TStartCypressTransaction>(
        bootstrap,
        context->Request(),
        context->GetAuthenticationIdentity())
            ->Apply());
}

void AbortCypressTransactionInSequoiaAndReply(
    TBootstrap* bootstrap,
    const ITransactionManager::TCtxAbortCypressTransactionPtr& context)
{
    context->ReplyFrom(New<TAbortCypressTransaction>(
        bootstrap,
        context->Request(),
        context->GetAuthenticationIdentity())
            ->Apply());
}

TFuture<TSharedRefArray> AbortExpiredCypressTransactionInSequoia(
    TBootstrap* bootstrap,
    TTransactionId transactionId)
{
    return New<TAbortCypressTransaction>(
        bootstrap,
        transactionId,
        /*force*/ false,
        NRpc::GetRootAuthenticationIdentity())
            ->Apply();
}

TFuture<TSharedRefArray> CommitCypressTransactionInSequoia(
    TBootstrap* bootstrap,
    TTransactionId transactionId,
    std::vector<TTransactionId> prerequisiteTransactionIds,
    TTimestamp commitTimestamp,
    NRpc::TAuthenticationIdentity authenticationIdentity)
{
    return New<TCommitCypressTransaction>(
        bootstrap,
        transactionId,
        std::move(prerequisiteTransactionIds),
        commitTimestamp,
        std::move(authenticationIdentity))
            ->Apply();
}

TFuture<void> ReplicateCypressTransactionsInSequoiaAndSyncWithLeader(
    NCellMaster::TBootstrap* bootstrap,
    TRange<TTransactionId> transactionIds)
{
    // Fast path.
    if (transactionIds.empty()) {
        return VoidFuture;
    }

    return New<TReplicateCypressTransactions>(
        bootstrap,
        transactionIds)
            ->Apply()
            .Apply(BIND([hydraManager = bootstrap->GetHydraFacade()->GetHydraManager()] {
                // NB: |sequoiaTransaction->Commit()| is set when Sequoia tx is
                // committed on leader (and some of followers). Since we want to
                // know when replicated tx is actually available on _this_ peer
                // sync with leader is needed.
                return hydraManager->SyncWithLeader();
            }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrasnactionServer
