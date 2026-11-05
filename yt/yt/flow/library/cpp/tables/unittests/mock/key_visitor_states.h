#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/tables/key_visitor_states.h>

#include <map>

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

//! In-memory implementation of IKeyVisitorStates for unit tests.
//! The transaction is ignored; Write() is applied immediately.
class TInMemoryKeyVisitorStates
    : public IKeyVisitorStates
{
public:
    TInMemoryKeyVisitorStates() = default;

    void Reconfigure(TDynamicTableRequestSpecPtr /*dynamicSpec*/) override
    { }

    void Write(
        NApi::IDynamicTableTransactionPtr transaction,
        const std::vector<std::pair<TTableKey, std::optional<TValue>>>& mutations) override;

    TFuture<TReadResult> Read(
        TTableKeyFilter filter,
        i64 limit,
        std::optional<TTableKey> offsetExclusive = std::nullopt) override;

    TFuture<std::vector<std::pair<TTableKey, TValue>>> ReadAll(TTableKeyFilter filter) override;

    // Test helpers.
    i64 GetWriteCount() const;
    i64 GetDeleteCount() const;
    i64 GetRowCount() const;

private:
    struct TTableKeyCompare
    {
        bool operator()(const TTableKey& a, const TTableKey& b) const;
    };

    std::map<TTableKey, TValue, TTableKeyCompare> Storage_;
    i64 WriteCount_ = 0;
    i64 DeleteCount_ = 0;
};

DEFINE_REFCOUNTED_TYPE(TInMemoryKeyVisitorStates);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
