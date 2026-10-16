#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/tables/key_states.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <map>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

// In-memory implementation of IKeyStates for unit testing.
// Transactions are ignored — Write() is applied immediately.
//
// Each row is stored as std::vector<TUnversionedOwningValue> indexed by column id.
// TUpdateMutation (= TUnversionedOwningRow) is applied by merging column values
// point-wise: for each value in the mutation, Storage_[key][value.Id] = value.
class TInMemoryKeyStates
    : public IKeyStates
{
public:
    TInMemoryKeyStates() = default;

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

    // Stores a row at |tableKey| with the given column values (indexed by column
    // id). Pass an empty vector when the test only cares about which keys exist.
    void Set(TTableKey tableKey, std::vector<NTableClient::TUnversionedOwningValue> values = {});

    // Test helpers.
    i64 GetWriteCount() const;
    i64 GetWrittenKeyCount() const;
    i64 GetLookupCount() const;
    i64 GetLoadedKeyCount() const;

    // When set, List() returns a future failed with this error.
    void SetListFailure(std::optional<TError> error);

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
    i64 LookupCount_ = 0;
    i64 LoadedKeyCount_ = 0;
    std::optional<TError> ListFailure_;
};

DEFINE_REFCOUNTED_TYPE(TInMemoryKeyStates);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
