#include "helpers.h"

#include "config.h"
#include "node_builder.h"
#include "node_visitor.h"

#include <library/yson/parser.h>
#include <library/yson/writer.h>
#include <library/yson/json_writer.h>

#include <library/json/json_reader.h>
#include <library/json/json_value.h>

#include "serialize.h"
#include "fluent.h"

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/stream/str.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TNode NodeFromYsonString(const Stroka& input, EYsonType type)
{
    TStringInput stream(input);
    TNode result;
    TNodeBuilder builder(&result);
    TYsonParser parser(&builder, &stream, type);
    parser.Parse();
    return result;
}

Stroka NodeToYsonString(const TNode& node, EYsonFormat format)
{
    TStringStream stream;
    TYsonWriter writer(&stream, format);
    TNodeVisitor visitor(&writer);
    visitor.Visit(node);
    return stream.Str();
}

Stroka NodeListToYsonString(const TNode::TList& nodes)
{
    TStringStream stream;
    TYsonWriter writer(&stream, YF_BINARY, YT_LIST_FRAGMENT);
    auto list = BuildYsonListFluently(&writer);
    for (const auto& node : nodes) {
        list.Item().Value(node);
    }
    return stream.Str();
}

TNode NodeFromYPath(const TRichYPath& path)
{
    return BuildYsonNodeFluently().BeginMap()
        .Item("path").Value(path)
    .EndMap();
}

Stroka AttributesToYsonString(const TNode& node)
{
    return BuildYsonStringFluently().BeginMap()
        .Item("attributes").Value(node)
    .EndMap();
}

Stroka AttributeFilterToYsonString(const TAttributeFilter& filter)
{
    return BuildYsonStringFluently().BeginMap()
        .Item("attributes").Value(filter)
    .EndMap();
}

void MergeNodes(TNode& dst, const TNode& src)
{
    if (dst.IsMap() && src.IsMap()) {
        auto& dstMap = dst.AsMap();
        const auto& srcMap = src.AsMap();
        for (const auto& srcItem : srcMap) {
            const auto& key = srcItem.first;
            auto dstItem = dstMap.find(key);
            if (dstItem != dstMap.end()) {
                MergeNodes(dstItem->second, srcItem.second);
            } else {
                dstMap[key] = srcItem.second;
            }
        }
    } else {
        if (dst.GetType() != src.GetType()) {
            dst = src;
        } else {
            if (src.HasAttributes()) {
                auto attributes = dst.GetAttributes();
                MergeNodes(attributes, src.GetAttributes());
                dst = src;
                dst.Attributes() = attributes;
            }
        }
    }
}

TYPath AddPathPrefix(const TYPath& path)
{
    if (path.has_prefix("//")) {
        return path;
    }
    return TConfig::Get()->Prefix + path;
}

TRichYPath AddPathPrefix(const TRichYPath& path)
{
    auto pathCopy(path);
    pathCopy.Path_ = AddPathPrefix(path.Path_);
    return pathCopy;
}

Stroka GetWriteTableCommand()
{
    return TConfig::Get()->ApiVersion == "v2" ? "write" : "write_table";
}

Stroka GetReadTableCommand()
{
    return TConfig::Get()->ApiVersion == "v2" ? "read" : "read_table";
}

Stroka GetWriteFileCommand()
{
    return TConfig::Get()->ApiVersion == "v2" ? "upload" : "write_file";
}

Stroka GetReadFileCommand()
{
    return TConfig::Get()->ApiVersion == "v2" ? "download" : "read_file";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
