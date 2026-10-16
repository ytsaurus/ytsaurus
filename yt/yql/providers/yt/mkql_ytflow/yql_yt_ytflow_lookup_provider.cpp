#include "yql_yt_ytflow_lookup_provider.h"
#include "yql_yt_ytflow_schema.h"

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/memory/new.h>

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <yt/yql/providers/ytflow/codec/yql_ytflow_input_codec.h>
#include <yt/yql/providers/ytflow/codec/yql_ytflow_output_codec.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/cache/rpc.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/library/arcadia_future_interop/interop.h>

#include <util/generic/ptr.h>
#include <util/string/join.h>


namespace NYql {

using namespace NKikimr::NMiniKQL;

namespace {

class TYtYtflowLookupProviderFactory;

class TYtLookupResult
    : public IYtflowLookupProvider::ILookupResult
{
public:
    explicit TYtLookupResult(NYT::NApi::TUnversionedLookupRowsResult result)
        : Result(std::move(result))
    {
    }

    NYT::NApi::TUnversionedLookupRowsResult Result;
};

class TYtYtflowLookupProvider
    : public IYtflowLookupProvider
{
public:
    TYtYtflowLookupProvider(
        const TYtYtflowLookupProviderFactory& factory,
        const IYtflowLookupProviderFactory::TCreationContext& ctx);

    NThreading::TFuture<ILookupResultPtr> Lookup(
        const TVector<NUdf::TUnboxedValue>& keys
    ) override {
        std::vector<NYT::NTableClient::TUnversionedRow> ytKeys;
        ytKeys.reserve(keys.size());

        for (const auto& key : keys) {
            ytKeys.push_back(OutputCodec->Convert(key));
        }

        auto rowBuffer = NYT::New<NYT::NTableClient::TRowBuffer>();
        rowBuffer->Absorb(std::move(*RowBuffer));
        RowBuffer->Clear();

        auto sharedRange = NYT::MakeSharedRange(std::move(ytKeys), std::move(rowBuffer));

        auto options = NYT::NApi::TLookupRowsOptions();
        options.KeepMissingRows = true;
        options.ColumnFilter = LookupColumnFilter;

        auto future = Client->LookupRows(
            TableName,
            LookupNameTable,
            std::move(sharedRange),
            std::move(options)
        ).Apply(BIND([](
            const NYT::NApi::TUnversionedLookupRowsResult& lookupRowsResult
        ) -> IYtflowLookupProvider::ILookupResultPtr {
            return std::make_shared<TYtLookupResult>(lookupRowsResult);
        }));

        return NYT::ToArcadiaFuture(future);
    }

    TVector<TVector<NUdf::TUnboxedValue>> Decode(
        const ILookupResultPtr& result
    ) override {
        const auto& lookupRowsResult =
            static_cast<const TYtLookupResult&>(*result).Result;

        MKQL_ENSURE(
            EqualTo(lookupRowsResult.Rowset->GetNameTable(), LookupNameTable),
            "Got unexpected name table as lookup result");

        TVector<TVector<NUdf::TUnboxedValue>> rowGroups;
        rowGroups.reserve(lookupRowsResult.Rowset->GetRows().size());

        for (const auto& row : lookupRowsResult.Rowset->GetRows()) {
            if (!row) {
                rowGroups.emplace_back();
                continue;
            }

            rowGroups.emplace_back(TVector<NUdf::TUnboxedValue>{
                InputCodec->Convert(row)
            });
        }

        return rowGroups;
    }

    TString GetTableName() const override {
        return FullTableName;
    }

private:
    NYT::NApi::IClientPtr Client;
    TString Cluster;
    TString TableName;
    TString FullTableName;

    NYT::NTableClient::TNameTablePtr LookupNameTable;
    NYT::NTableClient::TColumnFilter LookupColumnFilter;

    THolder<NYtflow::NCodec::IRowInputCodec> InputCodec;

    NYT::NTableClient::TRowBufferPtr RowBuffer;
    THolder<NYtflow::NCodec::IRowOutputCodec> OutputCodec;
};

class TYtYtflowLookupProviderFactory
    : public IYtflowLookupProviderFactory
{
public:
    explicit TYtYtflowLookupProviderFactory(
        const IYtflowLookupProviderRegistry::TFactoryCreationContext& ctx)
    {
        auto* args = AS_VALUE(TTupleLiteral, ctx.LookupSourceArgs);

        MKQL_ENSURE(
            args->GetValuesCount() == 3,
            "Unexpected values count: " << args->GetValuesCount());

        auto cluster = AS_VALUE(TDataLiteral, args->GetValue(0))->AsValue().AsStringRef();
        Cluster = TString(cluster.data(), cluster.size());

        auto tableName = AS_VALUE(TDataLiteral, args->GetValue(1))->AsValue().AsStringRef();
        TableName = TString(tableName.data(), tableName.size());

        auto tokenName = AS_VALUE(TDataLiteral, args->GetValue(2))->AsValue().AsStringRef();

        MKQL_ENSURE(
            ctx.SecureParamsProvider,
            "Secure params provider is not set");

        NUdf::TStringRef token;
        MKQL_ENSURE(
            ctx.SecureParamsProvider->GetSecureParam(
                tokenName, token),
            "Unknown token name: " << tokenName);
        Token = TString(token.data(), token.size());

        LookupSourceRowType = ctx.LookupSourceRowType;
        auto lookupYtType = ConvertType(LookupSourceRowType);
        auto reorderedLookupYtType = PartiallyReorderFields(
            lookupYtType,
            ctx.LookupSourceKeys);
        LookupTableSchema = BuildTableSchema(reorderedLookupYtType);

        KeysType = FilterFields(
            ctx.StreamRowType,
            ctx.StreamKeys,
            ctx.TypeEnvironment);
        auto keysYtType = ConvertType(KeysType);
        auto reorderedKeysYtType = PartiallyReorderFields(
            keysYtType,
            ctx.StreamKeys);
        KeysTableSchema = BuildTableSchema(reorderedKeysYtType);
    }

    THolder<IYtflowLookupProvider> Create(
        const TCreationContext& ctx) const override
    {
        return MakeHolder<TYtYtflowLookupProvider>(*this, ctx);
    }

private:
    friend class TYtYtflowLookupProvider;

    TString Cluster;
    TString TableName;
    TString Token;

    // These types are owned by the type environment of the graph or pattern,
    // which outlives the factory and providers created from it.
    const TStructType* LookupSourceRowType = nullptr;
    NYT::NTableClient::TTableSchemaPtr LookupTableSchema;

    const TType* KeysType = nullptr;
    NYT::NTableClient::TTableSchemaPtr KeysTableSchema;
};

TYtYtflowLookupProvider::TYtYtflowLookupProvider(
    const TYtYtflowLookupProviderFactory& factory,
    const IYtflowLookupProviderFactory::TCreationContext& ctx)
    : Cluster(factory.Cluster)
    , TableName(factory.TableName)
    , FullTableName(Join('.', Cluster, TableName))
    , RowBuffer(NYT::New<NYT::NTableClient::TRowBuffer>())
{
    auto connectionConfig = NYT::New<NYT::NApi::NRpcProxy::TConnectionConfig>();
    connectionConfig->SetDefaults();
    NYT::NClient::NCache::SetClusterUrl(connectionConfig, Cluster);
    Client = NYT::NClient::NCache::CreateClient(
        std::move(connectionConfig),
        NYT::NApi::TClientOptions::FromToken(factory.Token));

    LookupNameTable = NYT::NTableClient::TNameTable::FromSchema(
        *factory.LookupTableSchema);
    LookupColumnFilter = NYT::NTableClient::TColumnFilter(
        LookupNameTable->GetSize());

    InputCodec = NYtflow::NCodec::CreateRowInputCodec(
        factory.LookupSourceRowType,
        factory.LookupTableSchema,
        ctx.ValueBuilder,
        ctx.FunctionTypeInfoBuilder);

    OutputCodec = NYtflow::NCodec::CreateRowOutputCodec(
        factory.KeysType,
        factory.KeysTableSchema,
        RowBuffer);
}

} // anonymous namespace

void RegisterYtYtflowLookupProvider(IYtflowLookupProviderRegistry& registry) {
    registry.Register(
        TString(YtProviderName),
        [](const IYtflowLookupProviderRegistry::TFactoryCreationContext& ctx) {
            return MakeHolder<TYtYtflowLookupProviderFactory>(ctx);
        });
}

} // namespace NYql
