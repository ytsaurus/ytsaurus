#include "string_merger.h"

#include "forwarding_consumer.h"
#include "null_consumer.h"

#include <yt/yt/core/ypath/stack.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/tree_visitor.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <library/cpp/yt/yson_string/string.h>
#include <library/cpp/iterator/functools.h>

#include <util/stream/output.h>

namespace NYT::NYson {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TYsonStringMerger
    : public TForwardingYsonConsumer
{
public:
    //! TYsonStringMerger does not own YSON strings, so ensure that lifetime of YSON strings is long enough.
    TYsonStringMerger(
        IOutputStream* stream,
        std::vector<NYPath::TYPath> paths,
        std::vector<TYsonStringBuf> ysonStringBufs,
        EYsonFormat format)
        : Paths_(std::move(paths))
        , PathToIndex_(BuildPathToIndex(Paths_))
        , YsonWriter_(stream, format, EYsonType::Node, /*enableRaw*/ true)
    {
        SetYsonStringBufs(std::move(ysonStringBufs));
    }

    void OnMyStringScalar(TStringBuf /*value*/) override
    {
        YT_ABORT();
    }

    void OnMyInt64Scalar(i64 /*value*/) override
    {
        YT_ABORT();
    }

    void OnMyUint64Scalar(ui64 /*value*/) override
    {
        YT_ABORT();
    }

    void OnMyDoubleScalar(double /*value*/) override
    {
        YT_ABORT();
    }

    void OnMyBooleanScalar(bool /*value*/) override
    {
        YT_ABORT();
    }

    void OnMyEntity() override
    {
        YT_ABORT();
    }

    void OnMyBeginList() override
    {
        YT_ABORT();
    }

    void OnMyListItem() override
    {
        YT_ABORT();
    }

    void OnMyEndList() override
    {
        YT_ABORT();
    }

    void OnMyBeginMap() override
    {
        YsonWriter_.OnBeginMap();
        PathStack_.Push("");
    }

    void OnMyKeyedItem(TStringBuf key) override
    {
        YsonWriter_.OnKeyedItem(key);
        PathStack_.Pop();
        PathStack_.Push(TString{key});
        auto path = PathStack_.GetPath();
        auto it = PathToIndex_.find(path);
        if (it != PathToIndex_.end()) {
            YsonWriter_.OnRaw(YsonStringBufs_[it->second].AsStringBuf(), EYsonType::Node);
            Forward(GetNullYsonConsumer(), [] {});
        }
    }

    void OnMyEndMap() override
    {
        YsonWriter_.OnEndMap();
        PathStack_.Pop();
    }

    void OnMyBeginAttributes() override
    {
        YT_ABORT();
    }

    void OnMyEndAttributes() override
    {
        YT_ABORT();
    }

private:
    const std::vector<NYPath::TYPath> Paths_;
    const THashMap<NYPath::TYPath, ui64> PathToIndex_;

    TYsonWriter YsonWriter_;
    NYPath::TYPathStack PathStack_;
    std::vector<TYsonStringBuf> YsonStringBufs_;

    static THashMap<NYPath::TYPath, ui64> BuildPathToIndex(const std::vector<NYPath::TYPath>& paths)
    {
        THashMap<NYPath::TYPath, ui64> pathToIndex;
        pathToIndex.reserve(paths.size());
        for (const auto& [index, path] : Enumerate(paths)) {
            pathToIndex[path] = index;
        }
        return pathToIndex;
    }

    void SetYsonStringBufs(std::vector<TYsonStringBuf> ysonStringBufs)
    {
        YsonStringBufs_ = std::move(ysonStringBufs);
        for (auto ysonStringBuf : YsonStringBufs_) {
            if (ysonStringBuf.GetType() != EYsonType::Node) {
                THROW_ERROR_EXCEPTION(
                    "Yson string type can only be %v",
                    EYsonType::Node);
            }
        }
    }
};

std::tuple<NYPath::TYPath, NYTree::INodePtr> DoExpandLists(NYTree::INodePtr node, NYPath::TYPath path);

NYTree::INodePtr PutChildInPath(NYTree::INodePtr child, const NYPath::TYPath& path)
{
    if (path.empty()) {
        return child;
    }

    auto [newPath, newChild] = DoExpandLists(child, path);
    if (newPath.empty()) {
        return newChild;
    }
    auto node = NYTree::GetEphemeralNodeFactory()->CreateMap();
    ForceYPath(node, newPath);
    NYTree::SetNodeByYPath(node, newPath, newChild);
    return node;
}

std::tuple<NYPath::TYPath, NYTree::INodePtr> DoExpandLists(NYTree::INodePtr node, NYPath::TYPath path)
{
    auto it = path.find("*");
    if (it == NYPath::TYPath::npos) {
        return {path, node};
    }

    auto outPath = path.substr(0, std::max<size_t>(0, it - 1));
    auto inPath = path.substr(std::min(it + 1, path.size()), path.size());

    THROW_ERROR_EXCEPTION_UNLESS(node->GetType() == NYTree::ENodeType::List, "\"*\" can only expand lists");
    const auto list = node->AsList();

    auto newList = NYTree::GetEphemeralNodeFactory()->CreateList();
    auto children = list->GetChildren();
    for (auto& child : children) {
        list->RemoveChild(child);
        auto newChild = PutChildInPath(child, inPath);
        newList->AddChild(newChild);
    }

    return {outPath, newList};
}

std::optional<std::tuple<NYPath::TYPath, TYsonString>> ExpandLists(
    NYPath::TYPath path,
    TYsonStringBuf ysonStringBuf,
    EYsonFormat format)
{
    if (path.find('*') == NYPath::TYPath::npos) {
        return std::nullopt;
    }

    auto node = NYTree::ConvertToNode(ysonStringBuf);
    auto [newPath, newNode] = DoExpandLists(node, path);
    return {{newPath, ConvertToYsonString(newNode, format)}};
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TYsonString MergeYsonStrings(
    std::vector<NYPath::TYPath> paths,
    std::vector<TYsonStringBuf> ysonStringBufs,
    EYsonFormat format,
    bool expandLists)
{
    YT_VERIFY(paths.size() == ysonStringBufs.size());

    std::vector<TYsonString> forwardedStrings;
    for (const auto& [index, path] : Enumerate(paths)) {
        const auto& ysonStringBuf = ysonStringBufs[index];
        if (ysonStringBuf.GetType() != EYsonType::Node) {
            THROW_ERROR_EXCEPTION(
                "Yson string type can only be %v",
                EYsonType::Node);
        }

        if (expandLists) {
            auto result = ExpandLists(path, ysonStringBuf, format);
            if (result.has_value()) {
                auto& [newPath, newYsonString] = result.value();
                paths[index] = std::move(newPath);
                const auto& forwardedString = forwardedStrings.emplace_back(std::move(newYsonString));
                ysonStringBufs[index] = forwardedString;
            }
        }
    }
    auto rootNode = NYTree::GetEphemeralNodeFactory()->CreateMap();
    for (const auto& [index, path] : Enumerate(paths)) {
        if (path.empty()) {
            return TYsonString{ysonStringBufs[index]};
        }
        ForceYPath(rootNode, path);
        NYTree::SetNodeByYPath(rootNode, path, NYTree::GetEphemeralNodeFactory()->CreateMap());
    }
    TString result;
    size_t sizeEstimate = std::accumulate(
        ysonStringBufs.begin(),
        ysonStringBufs.end(),
        static_cast<size_t>(0),
        [] (size_t accumulated, TYsonStringBuf ysonStringBuf) {
            return accumulated + ysonStringBuf.AsStringBuf().size();
        });
    result.reserve(sizeEstimate);
    TStringOutput outputStream{result};
    TYsonStringMerger merger(&outputStream, std::move(paths), std::move(ysonStringBufs), format);
    NYTree::VisitTree(rootNode, &merger, /*stable*/ true);
    return TYsonString{result, EYsonType::Node};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
