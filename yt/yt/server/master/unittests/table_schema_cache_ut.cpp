#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/master/cell_server/public.h>

#include <yt/yt/server/master/table_server/compact_table_schema.h>
#include <yt/yt/server/master/table_server/table_schema_cache.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

namespace NYT::NTableServer {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TTableSchemaCacheTest
    : public ::testing::Test
{
public:
    TAsyncExpiringCacheConfigPtr CreateNeverExpiringAsyncExpiringCacheConfig()
    {
        auto config = New<TAsyncExpiringCacheConfig>();
        config->ExpireAfterAccessTime = TDuration::Days(128);
        config->ExpireAfterFailedUpdateTime = TDuration::Days(128);
        config->ExpireAfterSuccessfulUpdateTime = TDuration::Days(128);
        return config;
    }
};

TEST_F(TTableSchemaCacheTest, NormalSchema)
{
    auto tableSchemaCache = New<TTableSchemaCache>(CreateNeverExpiringAsyncExpiringCacheConfig());

    TTableSchema schema;
    auto compactSchema = New<TCompactTableSchema>(schema);

    auto schemaOrError = NConcurrency::WaitFor(tableSchemaCache->Get(compactSchema));
    EXPECT_TRUE(schemaOrError.IsOK());
    EXPECT_EQ(*schemaOrError.Value(), schema);
    // Schema should be cached.
    auto optionalSchema = tableSchemaCache->Find(compactSchema);
    EXPECT_TRUE(optionalSchema.has_value());
    auto cachedSchemaOrError = *optionalSchema;
    EXPECT_EQ(*cachedSchemaOrError.Value(), schema);
}

TEST_F(TTableSchemaCacheTest, DontCacheCorruptedSchema)
{
    auto tableSchemaCache = New<TTableSchemaCache>(CreateNeverExpiringAsyncExpiringCacheConfig());

    NTableClient::NProto::TTableSchemaExt protoSchema;
    auto* column = protoSchema.add_columns();
    column->set_name("foo");
    column->set_stable_name("foo");
    // Logical type is invalid.
    column->set_type(-1);
    auto corruptedSchema = New<TCompactTableSchema>(protoSchema);

    auto schemaOrError = NConcurrency::WaitFor(tableSchemaCache->Get(corruptedSchema));
    Cerr << Format(
        "Put compact table schema into cache (CompactTableSchema: %v, ParsedResult: %v)",
        corruptedSchema,
        schemaOrError);
    EXPECT_FALSE(schemaOrError.IsOK());
    EXPECT_EQ(schemaOrError.GetCode(), NCellServer::EErrorCode::CompactSchemaParseError);
    // Schema shouldn't be cached.
    EXPECT_EQ(tableSchemaCache->Find(corruptedSchema), std::nullopt);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableServer
