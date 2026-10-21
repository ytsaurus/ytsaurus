#include <yt/yt/flow/library/cpp/delta_codecs/codec.h>

#include <yt/yt/core/compression/codec.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <iostream>
#include <string>

namespace NYT::NFlow {

using namespace NYson;
using namespace NYTree;

void GenerateGoldenRows()
{
    const TYsonString base(std::string(R"({value=41;payload="a\x00\xff";nested={enabled=%true;};})"));
    const TYsonString target(std::string(R"({value=42;payload="b\x00\xff";nested={enabled=%false;};})"));
    const auto baseRef = TSharedRef::FromString(base.ToString());
    const auto targetRef = TSharedRef::FromString(target.ToString());
    const auto xdelta = NDeltaCodecs::GetCodec(NDeltaCodecs::ECodec::XDelta)->TryComputePatch(baseRef, targetRef).value();
    const auto vcdiff = NDeltaCodecs::GetCodec(NDeltaCodecs::ECodec::VCDiff)->TryComputePatch(baseRef, targetRef).value();

    std::vector<INodePtr> cases;
    const auto addCase = [&] (const std::string& name, const TYsonString& row, const TYsonString& expected, bool expectError = false) {
        cases.push_back(ConvertToNode(BuildYsonStringFluently()
                .BeginMap()
                .Item("name")
                .Value(name)
                .Item("row")
                .Value(row)
                .Item("expected_state")
                .Value(expected)
                .Item("expect_error")
                .Value(expectError)
                .EndMap()));
    };
    const auto packedRow = [&] (
        NCompression::ECodec compression,
        const TSharedRef& patch = TSharedRef::MakeEmpty(),
        NCompression::ECodec patchCompression = NCompression::ECodec::None,
        NDeltaCodecs::ECodec delta = NDeltaCodecs::ECodec::XDelta,
        bool explicitFormat = false) {
        auto compressed = NCompression::GetCodec(compression)->Compress(baseRef);
        auto compressedPatch = patch.Empty() ? patch : NCompression::GetCodec(patchCompression)->Compress(patch);
        return BuildYsonStringFluently()
            .BeginMap()
            .Item("computation_id")
            .Value("counter")
            .Item("key")
            .Value("some_key")
            .Item("name")
            .Value("/counts")
            .Item("state")
            .Entity()
            .Item("compressed")
            .Value(compressed.ToStringBuf())
            .Item("compressed_patch")
            .Value(compressedPatch.ToStringBuf())
            .DoIf(explicitFormat, [&] (auto fluent) {
                fluent
                    .Item("format")
                    .BeginMap()
                    .Item("compression")
                    .Value(compression)
                    .Item("patch_compression")
                    .Value(patchCompression)
                    .Item("delta")
                    .Value(delta)
                    .EndMap();
            })
            .EndMap();
    };
    const TYsonString entity(std::string("#"));
    addCase("empty", TYsonString(std::string("{computation_id=counter;name=\"/counts\";}")), entity);
    addCase(
        "plain",
        BuildYsonStringFluently()
            .BeginMap()
            .Item("key")
            .Value("some_key")
            .Item("state")
            .Value(base)
            .Item("compressed")
            .Entity()
            .Item("compressed_patch")
            .Entity()
            .EndMap(),
        base);
    addCase("default_zstd", packedRow(NCompression::ECodec::Zstd_6), base);
    auto nullFormat = ConvertTo<IMapNodePtr>(packedRow(NCompression::ECodec::Zstd_6));
    nullFormat->AddChild("format", ConvertToNode(entity));
    addCase("null_format_zstd", ConvertToYsonString(nullFormat), base);
    addCase("default_xdelta_patch", packedRow(NCompression::ECodec::Zstd_6, xdelta), target);
    addCase("compressed_xdelta_patch", packedRow(NCompression::ECodec::Zstd_3, xdelta, NCompression::ECodec::Zstd_6, NDeltaCodecs::ECodec::XDelta, true), target);
    addCase("vcdiff_patch", packedRow(NCompression::ECodec::Lz4, vcdiff, NCompression::ECodec::Lz4, NDeltaCodecs::ECodec::VCDiff, true), target);
    addCase("none_codecs", packedRow(NCompression::ECodec::None, TSharedRef::MakeEmpty(), NCompression::ECodec::None, NDeltaCodecs::ECodec::None, true), base);
    addCase("truncated_xdelta", packedRow(NCompression::ECodec::Zstd_6, TSharedRef::FromString(std::string("bad"))), entity, true);
    addCase("patch_without_base", TYsonString(std::string("{compressed=#;compressed_patch=\"bad\";}")), entity, true);
    addCase("invalid_zstd", TYsonString(std::string("{compressed=\"bad\";}")), entity, true);
    addCase("unknown_codec", TYsonString(std::string("{compressed=\"bad\";format={compression=unknown;};}")), entity, true);
    addCase("invalid_compressed_type", TYsonString(std::string("{compressed=123;}")), entity, true);
    addCase("invalid_patch_type", TYsonString(std::string("{compressed_patch=123;}")), entity, true);
    addCase("invalid_compressed_sentinel", TYsonString(std::string("{compressed=<type=min>#;}")), entity, true);
    addCase("invalid_patch_sentinel", TYsonString(std::string("{compressed_patch=<type=min>#;}")), entity, true);
    addCase("all_null_columns", TYsonString(std::string("{computation_id=#;key=#;name=#;state=#;compressed=#;compressed_patch=#;format=#;}")), entity);
    addCase("empty_packed_columns", TYsonString(std::string(R"({state=#;compressed="";compressed_patch="";format=#;})")), entity);
    addCase("null_base_empty_patch", TYsonString(std::string("{state=#;compressed=#;compressed_patch=\"\";format=#;}")), entity);
    addCase("empty_base_null_patch", TYsonString(std::string("{state=#;compressed=\"\";compressed_patch=#;format=#;}")), entity);
    addCase("invalid_compressed_unsigned", TYsonString(std::string("{compressed=123u;}")), entity, true);
    addCase("invalid_compressed_double", TYsonString(std::string("{compressed=0.25;}")), entity, true);
    addCase("invalid_compressed_boolean", TYsonString(std::string("{compressed=%true;}")), entity, true);
    addCase("invalid_compressed_list", TYsonString(std::string("{compressed=[1;];}")), entity, true);
    addCase("invalid_compressed_map", TYsonString(std::string("{compressed={value=1;};}")), entity, true);
    addCase("invalid_compressed_max_entity", TYsonString(std::string("{compressed=<type=max>#;}")), entity, true);
    addCase("invalid_compressed_int64_entity", TYsonString(std::string("{compressed=<type=int64>#;}")), entity, true);
    addCase("invalid_compressed_patch_unsigned", TYsonString(std::string("{compressed_patch=123u;}")), entity, true);
    addCase("invalid_compressed_patch_double", TYsonString(std::string("{compressed_patch=0.25;}")), entity, true);
    addCase("invalid_compressed_patch_boolean", TYsonString(std::string("{compressed_patch=%true;}")), entity, true);
    addCase("invalid_compressed_patch_list", TYsonString(std::string("{compressed_patch=[1;];}")), entity, true);
    addCase("invalid_compressed_patch_map", TYsonString(std::string("{compressed_patch={value=1;};}")), entity, true);
    addCase("invalid_compressed_patch_max_entity", TYsonString(std::string("{compressed_patch=<type=max>#;}")), entity, true);
    addCase("invalid_compressed_patch_int64_entity", TYsonString(std::string("{compressed_patch=<type=int64>#;}")), entity, true);
    addCase("invalid_format_signed", TYsonString(std::string("{format=123;}")), entity, true);
    addCase("invalid_format_unsigned", TYsonString(std::string("{format=123u;}")), entity, true);
    addCase("invalid_format_double", TYsonString(std::string("{format=0.25;}")), entity, true);
    addCase("invalid_format_boolean", TYsonString(std::string("{format=%true;}")), entity, true);
    addCase("invalid_format_list", TYsonString(std::string("{format=[1;];}")), entity, true);
    addCase("default_format_map", TYsonString(std::string("{format={value=1;};}")), entity);
    addCase("invalid_format_compression_type", TYsonString(std::string("{format={compression=[1;];};}")), entity, true);
    addCase("invalid_format_string", TYsonString(std::string("{format=\"wrong\";}")), entity, true);
    addCase("invalid_state_typed_entity", TYsonString(std::string("{state=<type=int64>#;}")), entity, true);
    addCase("invalid_format_typed_entity", TYsonString(std::string("{format=<type=int64>#;}")), entity, true);
    std::cout << ConvertToYsonString(cases, EYsonFormat::Pretty).ToString() << std::endl;
}

} // namespace NYT::NFlow

int main()
{
    NYT::NFlow::GenerateGoldenRows();
}
