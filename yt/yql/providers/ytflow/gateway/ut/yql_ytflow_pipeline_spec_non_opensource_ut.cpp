#include <yt/yql/providers/ytflow/gateway/yql_ytflow_pipeline_spec.h>

#include <yql/essentials/minikql/mkql_function_registry.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/gtest/gtest.h>


namespace NYql::NYtflow::NPrivate {
namespace {

TUserDataBlock MakeUserDataBlock(std::initializer_list<EUserDataBlockUsage> usages)
{
    TUserDataBlock block;
    for (auto usage : usages) {
        block.Usage.Set(usage);
    }
    return block;
}

TEST(TPipelineUdfPaths, DistinctPathsPreserveModuleConflict)
{
    TUserDataTable userDataBlocks;
    userDataBlocks.emplace(
        TUserDataKey::Udf(
            "yql/essentials/udfs/common/ip_base/libip_udf.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Udf}));
    userDataBlocks.emplace(
        TUserDataKey::Udf(
            "yql/udfs/common/ip/libip_udf.so"_sb),
        MakeUserDataBlock({EUserDataBlockUsage::Udf}));

    using namespace NKikimr::NMiniKQL;

    auto udfPaths = BuildPipelineUdfPaths(userDataBlocks);
    ASSERT_EQ(2u, udfPaths.size());
    EXPECT_EQ("yql/essentials/udfs/common/ip_base/libip_udf.so", udfPaths[0]);
    EXPECT_EQ("yql/udfs/common/ip/libip_udf.so", udfPaths[1]);
    for (auto& udfPath : udfPaths) {
        udfPath = BinaryPath(udfPath);
    }

    ASSERT_THROW_MESSAGE_HAS_SUBSTR(
        CreateFunctionRegistry(
            {},
            IBuiltinFunctionRegistry::TPtr(),
            /*allowUdfPatch*/ false,
            udfPaths),
        yexception,
        "UDF module duplication: name Ip");
}

} // anonymous namespace
} // namespace NYql::NYtflow::NPrivate
