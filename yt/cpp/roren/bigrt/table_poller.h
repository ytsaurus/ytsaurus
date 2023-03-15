#pragma once

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/table_client/name_table.h>

#include <kernel/yt/dynamic/proto_api.h>

#include <util/generic/string.h>
#include <util/system/spinlock.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename TProto>
std::any ParseRowset(const NYT::NApi::IUnversionedRowsetPtr& rowset, int responseKeyColumnId);

using TParseRowsetFun = std::function<std::any(const NYT::NApi::IUnversionedRowsetPtr& rowset, int responseKeyColumnId)>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

class TTablePoller
    : public TThrRefBase
{
private:
    using TKey = std::tuple<TString, TString, const ::google::protobuf::Descriptor*>;

    class TPoller
        : public TThrRefBase
    {
    public:
        TPoller(
            NYT::NApi::IClientPtr client,
            const TString& table,
            TDuration refreshPeriod,
            NYT::IInvokerPtr invoker,
            NPrivate::TParseRowsetFun parseRowFun);

        void SetPeriod(TDuration refreshPeriod);
        NYT::TFuture<std::any> AsyncGetParsedRows();
        void Finish();

    private:
        NYT::NApi::IClientPtr Client_;
        TString TablePath_;

        NPrivate::TParseRowsetFun ParseRowsetFun_;

        TDuration RefreshPeriod_;
        NYT::NConcurrency::TPeriodicExecutorPtr RefreshExecutor_;
        NYT::TPromise<void> FirstRefreshPromise_;
        NYT::TFuture<void> FirstRefreshFuture_;

        TSpinLock ParsedRowsLock_;
        std::any ParsedRows_;

        // cached data
        NYT::NTableClient::TTableSchemaPtr Schema_;
        NYT::NTableClient::TNameTablePtr NameTable_;
        int KeyColumnId_;

    private:
        void Refresh();
        void FetchSchema();
    };

public:
    explicit TTablePoller(NYT::IInvokerPtr invoker);
    ~TTablePoller();

    template <typename TProto>
    void Register(NYT::NApi::IClientPtr client, const TString& cluster, const TString& table, TDuration refreshPeriod);

    template <typename TProto>
    NYT::TFuture<std::shared_ptr<THashMap<TString, TProto>>> AsyncGetHashMap(const TString& cluster, const TString& table);

private:
    NYT::IInvokerPtr Invoker_;

    TAdaptiveLock PollersLock_;
    THashMap<TKey, ::TIntrusivePtr<TPoller>> Pollers_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TProto>
void TTablePoller::Register(
    NYT::NApi::IClientPtr client,
    const TString& cluster,
    const TString& table,
    TDuration refreshPeriod)
{
    auto key = TKey{cluster, table, TProto::descriptor()};
    auto guard = Guard(PollersLock_);
    auto it = Pollers_.find(key);
    if (it != Pollers_.end()) {
        it->second->SetPeriod(refreshPeriod);
        return;
    }
    auto poller = ::MakeIntrusive<TPoller>(
        std::move(client),
        table,
        refreshPeriod,
        Invoker_,
        &NPrivate::ParseRowset<TProto>);
    Pollers_.emplace(std::move(key), std::move(poller));
}

template <typename TProto>
NYT::TFuture<std::shared_ptr<THashMap<TString, TProto>>> TTablePoller::AsyncGetHashMap(
    const TString& cluster,
    const TString& table)
{
    auto key = TKey{cluster, table, TProto::descriptor()};
    auto guard = Guard(PollersLock_);
    auto it = Pollers_.find(key);
    Y_VERIFY(it != Pollers_.end(), "Not registerd key: %s, %s, %s", cluster.c_str(), table.c_str(), typeid(TProto).name());
    return it->second->AsyncGetParsedRows()
        .Apply(BIND([] (std::any parsedRowsAny) {
            return std::any_cast<std::shared_ptr<THashMap<TString, TProto>>>(parsedRowsAny);
        }));
}

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename TProto>
std::any ParseRowset(const NYT::NApi::IUnversionedRowsetPtr& rowset, int responseKeyColumnId)
{
    auto result = std::make_shared<THashMap<TString, TProto>>();
    auto protoSchema = NYT::NProtoApi::TProtoSchema{TProto::descriptor(), *rowset->GetSchema()};
    for (const auto& row : rowset->GetRows()) {
        auto key = row[responseKeyColumnId];
        TProto msg;
        protoSchema.RowToMessage(row, &msg);
        result->emplace(key.AsString(), std::move(msg));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
