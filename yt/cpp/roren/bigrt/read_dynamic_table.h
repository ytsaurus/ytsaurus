#pragma once

#include "bigrt_execution_context.h"
#include "bind_to_dict.h"
#include "table_poller.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/cpp/roren/bigrt/graph/parser.h>
#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/transforms/dict_join.h>

#include <kernel/yt/dynamic/proto_api.h>

#include <util/generic/string.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename TProto>
TTransform<void, TKV<TString, TProto>> BindToProtoDynamicTable(TString cluster, TString path);

////////////////////////////////////////////////////////////////////////////////

// IMPLEMENTATION

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

template <typename TProto>
class TProtoDynTableDictResolver
    : public IDictResolver<TString, TProto>
{
public:
    TProtoDynTableDictResolver(TString cluster, TString tablePath)
        : Cluster_(std::move(cluster))
        , TablePath_(std::move(tablePath))
    { }

    std::vector<std::optional<TProto>> Resolve(std::span<const TString> keys, const IExecutionContextPtr& executionContext) override
    {
        auto client = NPrivate::ResolveYtClient(executionContext->As<IBigRtExecutionContext>(), Cluster_);
        if (!Schema_) {
            NYT::NApi::TGetNodeOptions options;
            options.Timeout = TDuration::Seconds(10);
            auto getNodeResult = client->GetNode(TablePath_ + "/@schema", options);
            auto schemaNode = NYT::NConcurrency::WaitFor(getNodeResult)
                .ValueOrThrow();

            Schema_ = NYT::NYTree::ConvertTo<NYT::NTableClient::TTableSchemaPtr>(schemaNode);
            NameTable_ = NYT::NTableClient::TNameTable::FromSchema(*Schema_);

            std::vector<TString> keyColumnNames;
            for (const auto& column: Schema_->Columns()) {
                if (column.SortOrder() && !column.Expression()) {
                    keyColumnNames.push_back(column.Name());
                }
            }

            Y_VERIFY(keyColumnNames.size() == 1, "");
            KeyColumnId_ = NameTable_->GetIdOrThrow(keyColumnNames[0]);
        }

        auto rowBuffer = NYT::New<NYT::NTableClient::TRowBuffer>();
        std::vector<NYT::NTableClient::TUnversionedRow> unversionedKeys;
        unversionedKeys.reserve(keys.size());

        for (auto& key: keys) {
            NYT::NTableClient::TUnversionedRowBuilder builder;
            builder.AddValue(NYT::NTableClient::MakeUnversionedStringValue(key, KeyColumnId_));
            unversionedKeys.push_back(rowBuffer->CaptureRow(builder.GetRow()));
        }

        const auto unversionedKeyRange = NYT::MakeSharedRange(std::move(unversionedKeys), std::move(rowBuffer));

        NYT::NApi::TLookupRowsOptions options;
        options.Timeout = TDuration::Seconds(10);
        options.KeepMissingRows = true;

        auto lookupResult = client->LookupRows(TablePath_, NameTable_, unversionedKeyRange, options);

        auto rowset = NYT::NConcurrency::WaitFor(lookupResult)
            .ValueOrThrow();

        std::vector<std::optional<TProto>> result;
        result.reserve(keys.size());

        auto protoSchema = NYT::NProtoApi::TProtoSchema{TProto::descriptor(), *rowset->GetSchema()};
        auto resultNameTable = rowset->GetNameTable();
        for (const auto& row : rowset->GetRows()) {
            if (row) {
                result.push_back(TProto{});
                protoSchema.RowToMessage(row, &*result.back());
            } else {
                result.push_back({});
            }
        }
        return result;
    }

    typename IDictResolver<TString, TProto>::TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IDictResolverPtr<TString, TProto> {
            return ::MakeIntrusive<TProtoDynTableDictResolver<TProto>>(TString{}, TString{});
        };
    }

    void SaveState(IOutputStream& out) const override
    {
        ::Save(&out, Cluster_);
        ::Save(&out, TablePath_);
    }

    void LoadState(IInputStream& in) override
    {
        ::Load(&in, Cluster_);
        ::Load(&in, TablePath_);
    }

private:
    TString Cluster_;
    TString TablePath_;

    // cached data
    NYT::NTableClient::TTableSchemaPtr Schema_;
    NYT::NTableClient::TNameTablePtr NameTable_;
    int KeyColumnId_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TProto>
class TProtoDynTableInMemoryDictResolver
    : public IDictResolver<TString, TProto>
{
public:
    TProtoDynTableInMemoryDictResolver(
        TString cluster,
        TString tablePath,
        TDuration refreshPeriod)
        : Cluster_(move(cluster))
        , TablePath_(tablePath)
        , RefreshPeriod_(refreshPeriod)
    { }

    void Start(const IExecutionContextPtr& executionContext) override
    {
        if (Started_) {
            return;
        }
        auto context = ::TIntrusivePtr(executionContext->As<IBigRtExecutionContext>());
        auto client = ResolveYtClient(context, this->Cluster_);
        auto tablePoller = GetTablePoller(context);
        tablePoller->Register<TProto>(std::move(client), this->Cluster_, this->TablePath_, RefreshPeriod_);

        Started_ = true;
        TablePollerPtr_ = tablePoller.Get();
    }

    std::vector<std::optional<TProto>> Resolve(std::span<const TString> keys, const IExecutionContextPtr& executionContext) override
    {
        auto context = ::TIntrusivePtr(executionContext->As<IBigRtExecutionContext>());
        auto tablePoller = GetTablePoller(context);
        Y_VERIFY(
            tablePoller.Get() == TablePollerPtr_,
            "%s", NYT::Format("%v != %v", TablePollerPtr_, tablePoller.Get()).c_str());
        auto hashMap = NYT::NConcurrency::WaitFor(
            tablePoller->AsyncGetHashMap<TProto>(this->Cluster_, this->TablePath_))
            .ValueOrThrow();

        std::vector<std::optional<TProto>> result;
        result.reserve(keys.size());
        for (const auto& key : keys) {
            auto it = hashMap->find(key);
            if (it == hashMap->end()) {
                result.emplace_back();
            } else {
                result.push_back(it->second);
            }
        }
        return result;
    }

    typename IDictResolver<TString, TProto>::TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IDictResolverPtr<TString, TProto> {
            return ::MakeIntrusive<TProtoDynTableInMemoryDictResolver<TProto>>(TString{}, TString{}, TDuration{});
        };
    }

     void SaveState(IOutputStream& out) const override
    {
        ::Save(&out, Cluster_);
        ::Save(&out, TablePath_);
        ::Save(&out, RefreshPeriod_);
    }

    void LoadState(IInputStream& in) override
    {
        ::Load(&in, Cluster_);
        ::Load(&in, TablePath_);
        ::Load(&in, RefreshPeriod_);
    }

private:
    TString Cluster_;
    TString TablePath_;
    TDuration RefreshPeriod_;

    // Just for check of correctness.
    const TTablePoller* TablePollerPtr_ = nullptr;

    bool Started_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <typename TProto>
TTransform<void, TKV<TString, TProto>> BindToProtoDynamicTable(TString cluster, TString path)
{
    return BindToDict<NPrivate::TProtoDynTableDictResolver<TProto>>(cluster, path);
}

template <typename TProto>
TTransform<void, TKV<TString, TProto>> BindToProtoDynamicTableInMemory(TString cluster, TString path, TDuration refreshPeriod)
{
    return BindToDict<NPrivate::TProtoDynTableInMemoryDictResolver<TProto>>(cluster, path, refreshPeriod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
