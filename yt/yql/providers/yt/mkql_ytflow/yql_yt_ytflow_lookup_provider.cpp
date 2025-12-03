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

class TYtYtflowLookupProvider
    : public IYtflowLookupProvider
{
public:
    TYtYtflowLookupProvider(IYtflowLookupProviderRegistry::TCreationContext& ctx)
        : RowBuffer(NYT::New<NYT::NTableClient::TRowBuffer>())
    {
        auto* args = AS_VALUE(TTupleLiteral, ctx.LookupSourceArgs);

        MKQL_ENSURE(
            args->GetValuesCount() == 3,
            "Unexpected values count: " << args->GetValuesCount());

        auto clusterData = args->GetValue(0);
        auto cluster = AS_VALUE(TDataLiteral, clusterData)->AsValue().AsStringRef();
        Cluster = TString(cluster.data(), cluster.size());

        auto tableNameData = args->GetValue(1);
        auto tableName = AS_VALUE(TDataLiteral, tableNameData)->AsValue().AsStringRef();
        TableName = TString(tableName.data(), tableName.size());

        FullTableName = Join('.', Cluster, TableName);

        auto tokenNameData = args->GetValue(2);
        auto tokenName = AS_VALUE(TDataLiteral, tokenNameData)->AsValue().AsStringRef();

        MKQL_ENSURE(
            ctx.ComputationNodeFactoryContext.SecureParamsProvider,
            "Secure params provider is not set");

        NUdf::TStringRef token;
        MKQL_ENSURE(
            ctx.ComputationNodeFactoryContext.SecureParamsProvider->GetSecureParam(
                tokenName, token),
            "Unknown token name: " << tokenName);

        {
            auto connectionConfig = NYT::New<NYT::NApi::NRpcProxy::TConnectionConfig>();
            connectionConfig->SetDefaults();

            NYT::NClient::NCache::SetClusterUrl(
                connectionConfig,
                TString(cluster.data(), cluster.size()));

            Client = NYT::NClient::NCache::CreateClient(
                std::move(connectionConfig),
                NYT::NApi::TClientOptions::FromToken(
                    TString(token.data(), token.size())));
        }

        {
            auto ytType = ConvertType(ctx.LookupSourceRowType);
            auto reorderedYtType = PartiallyReorderFields(ytType, ctx.LookupSourceKeys);
            auto tableSchema = BuildTableSchema(reorderedYtType);

            LookupNameTable = NYT::NTableClient::TNameTable::FromSchema(*tableSchema);
            LookupColumnFilter = NYT::NTableClient::TColumnFilter(
                LookupNameTable->GetSize());

            InputCodec = NYtflow::NCodec::CreateInputCodec(
                ctx.LookupSourceRowType,
                std::move(tableSchema),
                const_cast<NUdf::IValueBuilder&>(
                    *ctx.ComputationNodeFactoryContext.Builder),
                ctx.FunctionTypeInfoBuilder);
        }

        {
            const auto* keysType = FilterFields(
                ctx.StreamRowType,
                ctx.StreamKeys,
                ctx.ComputationNodeFactoryContext.Env);

            auto keysYtType = ConvertType(keysType);
            auto keysTableSchema = BuildTableSchema(keysYtType);

            OutputCodec = NYtflow::NCodec::CreateOutputCodec(
                keysType, std::move(keysTableSchema), RowBuffer);
        }
    }

    NThreading::TFuture<TLookupResultCallback> Lookup(
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
        ).Apply(BIND([this](
            const NYT::NApi::TUnversionedLookupRowsResult& lookupRowsResult
        ) -> IYtflowLookupProvider::TLookupResultCallback {
            return [this, lookupRowsResult] {
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
            };
        }));

        return NYT::ToArcadiaFuture(future);
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

    THolder<NYtflow::NCodec::IInputCodec> InputCodec;

    NYT::NTableClient::TRowBufferPtr RowBuffer;
    THolder<NYtflow::NCodec::IOutputCodec> OutputCodec;
};

} // anonymous namespace

void RegisterYtYtflowLookupProvider(IYtflowLookupProviderRegistry& registry) {
    registry.Register(
        TString(YtProviderName),
        [](IYtflowLookupProviderRegistry::TCreationContext& ctx) {
            return MakeHolder<TYtYtflowLookupProvider>(ctx);
        });
}

} // namespace NYql
