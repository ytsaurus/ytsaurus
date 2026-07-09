#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/tables/partition_states.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <map>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

// In-memory implementation of IPartitionStates for unit testing.
// Transactions are ignored — Write() is applied immediately.
//
// Each row is stored as std::vector<TUnversionedOwningValue> indexed by column id.
// TUpdateMutation (= TUnversionedOwningRow) is applied by merging column values
// point-wise: for each value in the mutation, Storage_[key][value.Id] = value.
class TInMemoryPartitionStates
    : public IPartitionStates
{
public:
    TInMemoryPartitionStates() = default;

    void Reconfigure(TDynamicTableRequestSpecPtr /*dynamicSpec*/) override
    { }

    TFuture<THashMap<TTableKey, NYsonSerializer::TStatePtr>> Lookup(
        THashSet<TTableKey> keys,
        std::optional<std::string> tag = std::nullopt) override;

    void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const THashMap<TTableKey, NYsonSerializer::TStateMutation>& mutations,
        std::optional<std::string> tag = std::nullopt) override;

    TFuture<TListResult> List(
        TTableKeyFilter filter,
        i64 limit,
        std::optional<TTableKey> offsetExclusive = std::nullopt) override;

    TFuture<std::vector<TTableKey>> ListAll(TTableKeyFilter filter) override;

    // Test helpers: number of non-empty Write() mutations applied.
    i64 GetWriteCount() const;
    i64 GetWrittenKeyCount() const;

private:
    struct TTableKeyCompare
    {
        bool operator()(const TTableKey& a, const TTableKey& b) const;
    };

    // Each row is stored as a vector of owned values, indexed by column id.
    // The vector is resized as needed when new column ids appear.
    std::map<TTableKey, std::vector<NTableClient::TUnversionedOwningValue>, TTableKeyCompare> Storage_;
    i64 WriteCount_ = 0;
    i64 WrittenKeyCount_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TInMemoryPartitionStates);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
