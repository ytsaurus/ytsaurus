#pragma once

#include "private.h"

#include "cluster_tracker.h"
#include "table_schema.h"
#include "subquery_spec.h"

#include <yt/server/lib/chunk_pools/chunk_stripe.h>

#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TStorageDistributedBase
    : public DB::IStorage
{
public:
    TStorageDistributedBase(
        NTableClient::TTableSchema readSchema,
        TClickHouseTableSchema clickHouseSchema)
        : ClickHouseSchema(std::move(clickHouseSchema))
        , ReadSchema(std::move(readSchema))
    { }

    virtual void startup() override;

    std::string getName() const override;

    bool isRemote() const override;

    virtual bool supportsIndexForIn() const override;

    virtual bool mayBenefitFromIndexForIn(const DB::ASTPtr& /* left_in_operand */, const DB::Context& /* query_context */) const override;

    DB::QueryProcessingStage::Enum getQueryProcessingStage(const DB::Context& context) const override;

    DB::BlockInputStreams read(
        const DB::Names& columnNames,
        const DB::SelectQueryInfo& queryInfo,
        const DB::Context& context,
        DB::QueryProcessingStage::Enum processedStage,
        size_t maxBlockSize,
        unsigned numStreams) override;

    virtual bool supportsSampling() const override;

protected:
    virtual std::vector<NYPath::TRichYPath> GetTablePaths() const = 0;

    // TODO(max42): why is this different for different descendants?
    virtual DB::ASTPtr RewriteSelectQueryForTablePart(
        const DB::ASTPtr& queryAst,
        const std::string& subquerySpec) = 0;

    const TClickHouseTableSchema& GetSchema() const;

private:
    TClickHouseTableSchema ClickHouseSchema;
    NTableClient::TTableSchema ReadSchema;
    TSubquerySpec SpecTemplate;
    NChunkPools::TChunkStripeListPtr StripeList;

    void Prepare(
        int subqueryCount,
        const DB::SelectQueryInfo& queryInfo,
        const DB::Context& context);

    static DB::Settings PrepareLeafJobSettings(const DB::Settings& settings);

    static DB::ThrottlerPtr CreateNetThrottler(const DB::Settings& settings);

    static DB::BlockInputStreamPtr CreateLocalStream(
        const DB::ASTPtr& queryAst,
        const DB::Context& context,
        DB::QueryProcessingStage::Enum processedStage);

    static DB::BlockInputStreamPtr CreateRemoteStream(
        const IClusterNodePtr& remoteNode,
        const DB::ASTPtr& queryAst,
        const DB::Context& context,
        const DB::ThrottlerPtr& throttler,
        const DB::Tables& externalTables,
        DB::QueryProcessingStage::Enum processedStage);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
