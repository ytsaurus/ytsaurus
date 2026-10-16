#include "draw_pipeline_graph.h"
#include "fill_graph_limits.h"

#include <yt/yt/flow/library/cpp/common/internal_urls.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <algorithm>
#include <vector>

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

namespace {

const std::string WarningNodeStyle = "fill:#ffcccc,color:#333,stroke:#cc0000";
const std::string WarningLinkStyle = "stroke:#cc0000,stroke-width:2px";
const std::string ReadDelayLinkStyle = "stroke:#ccaa00,stroke-width:1px,stroke-dasharray:5 5";

////////////////////////////////////////////////////////////////////////////////

// Convert an arbitrary string into a valid Mermaid node identifier.
// Replaces all characters that are not [a-zA-Z0-9_] with '_'.
std::string MermaidSafeId(std::string_view raw)
{
    std::string result;
    result.reserve(raw.size());
    for (char c : raw) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
            result += c;
        } else {
            result += '_';
        }
    }
    return result;
}

// Sanitize text for use inside a Mermaid node label (inside double-quotes).
// Uses HTML entities for characters that break Mermaid parsing.
// Mermaid supports HTML entities inside double-quoted labels.
std::string MermaidSafeLabel(const std::string& text)
{
    std::string result;
    result.reserve(text.size() * 2);
    size_t i = 0;
    while (i < text.size()) {
        // Check for "::" (C++ namespace separator) — must come before single ":".
        if (i + 1 < text.size() && text[i] == ':' && text[i + 1] == ':') {
            result += "&colon;&colon;";
            i += 2;
            continue;
        }
        unsigned char c = static_cast<unsigned char>(text[i]);
        // Handle multi-byte UTF-8 sequences — pass through unchanged.
        if (c >= 0x80) {
            result += text[i];
            ++i;
            continue;
        }
        switch (text[i]) {
            case '"':
                result += "&quot;";
                break;
            case ':':
                result += "&colon;";
                break;
            case '(':
                result += "&lpar;";
                break;
            case ')':
                result += "&rpar;";
                break;
            case '[':
                result += "&lbrack;";
                break;
            case ']':
                result += "&rbrack;";
                break;
            case '{':
                result += "&lbrace;";
                break;
            case '}':
                result += "&rbrace;";
                break;
            case '=':
                result += "&equals;";
                break;
            case '#':
                result += "&num;";
                break;
            case '<':
                result += "&lt;";
                break;
            case '>':
                result += "&gt;";
                break;
            default:
                result += text[i];
                break;
        }
        ++i;
    }
    return result;
}

// Sanitize text for use inside a Mermaid edge label (inside -->|...|).
std::string MermaidSafeEdgeLabel(const std::string& text)
{
    std::string result;
    result.reserve(text.size());
    for (char c : text) {
        if (c == '|') {
            result += '/';
        } else if (c == '"') {
            result += '\'';
        } else if (c == '{') {
            result += "&lbrace;";
        } else if (c == '}') {
            result += "&rbrace;";
        } else {
            result += c;
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool IsComputationWarning(const TPipelineComputationDescription& computation)
{
    return computation.HighlightCpuUsage || computation.Status >= NLogging::ELogLevel::Warning || computation.UnstablePartitionCount > 0;
}

// Build computation node label.
std::string MakeComputationLabel(const TPipelineComputationDescription& computation)
{
    TStringBuilder sb;
    sb.AppendString(computation.Name);
    sb.AppendChar('\n');
    sb.AppendString(computation.ClassName);

    if (!computation.GroupBySchemaStr.empty()) {
        sb.AppendFormat("\nGroupBy: %v", computation.GroupBySchemaStr);
    }

    if (!computation.StatePath.empty()) {
        sb.AppendFormat("\nState: %v", computation.StatePath);
    }

    if (!computation.ReadDelays.empty()) {
        sb.AppendString("\nReadDelays:");
        std::vector<std::pair<TStreamId, TDuration>> sortedDelays(computation.ReadDelays.begin(), computation.ReadDelays.end());
        std::sort(sortedDelays.begin(), sortedDelays.end(), [] (const auto& a, const auto& b) {
            return a.first < b.first;
        });
        for (const auto& [streamId, delay] : sortedDelays) {
            sb.AppendFormat("\n  %Qv: %v", streamId.Underlying(), delay);
        }
    }

    if (IsComputationWarning(computation)) {
        if (computation.HighlightCpuUsage) {
            sb.AppendFormat("\n\xe2\x9a\xa0 MaxCpuUsage %.3f", computation.CpuUsage); // ⚠
        }
        if (computation.UnstablePartitionCount > 0) {
            sb.AppendFormat("\n\xe2\x9a\xa0 UnstablePartitions: %v/%v", // ⚠
                computation.UnstablePartitionCount,
                computation.TotalPartitionCount);
        }
        if (computation.Status >= NLogging::ELogLevel::Warning) {
            sb.AppendString("\n\xe2\x9a\xa0 HasProblems"); // ⚠
        }
    }

    return MermaidSafeLabel(sb.Flush());
}

// Build stream node label.
// Uses stream.GlobalName (= globalStreamId, without StreamPrefix) for unambiguous human-readable display.
std::string MakeStreamLabel(const TStream& stream)
{
    TStringBuilder sb;
    sb.AppendString(stream.GlobalName.Underlying());
    if (stream.SourceGraphEntityId) {
        sb.AppendString("\n(source)");
    }
    if (stream.SinkGraphEntityId) {
        sb.AppendString("\n(sink)");
    }
    return MermaidSafeLabel(sb.Flush());
}

////////////////////////////////////////////////////////////////////////////////

// Sorted keys of a hash map (native key type), for deterministic output.
template <class TMap>
auto SortedKeys(const TMap& map)
{
    auto keys = GetKeys(map);
    std::sort(keys.begin(), keys.end());
    return keys;
}

// Sorted elements of a hash set (native element type), for deterministic output.
template <class TSet>
auto SortedElements(const TSet& set)
{
    std::vector<typename TSet::value_type> elements(set.begin(), set.end());
    std::sort(elements.begin(), elements.end());
    return elements;
}

////////////////////////////////////////////////////////////////////////////////

// Accumulates Mermaid flowchart content and emits the final string.
// Tracks edge indices for linkStyle application.
struct TMermaidBuilder
{
    TStringBuilder Out;
    int EdgeIndex = 0;
    std::vector<int> WarningEdgeIndices;
    std::vector<int> ReadDelayEdgeIndices;

    void EmitStreamNode(const TStream& stream)
    {
        std::string nodeId = "s_" + MermaidSafeId(stream.Id.Underlying());
        Out.AppendFormat("  %v([\"%v\"])\n", nodeId, MakeStreamLabel(stream));
    }

    void EmitComputationNode(const std::string& nodeId, const TPipelineComputationDescription& computation)
    {
        Out.AppendFormat("  %v[\"%v\"]\n", nodeId, MakeComputationLabel(computation));
    }

    // Emit a directed edge from→to with optional fill-rate label.
    // Returns the edge index (for linkStyle).
    int EmitEdge(
        const std::string& fromNode,
        const std::string& toNode,
        const THashMap<TGraphEntityId, THashMap<std::string, TStreamLimitStats>>& limitStats,
        const TGraphEntityId& streamId,
        double writerBlocked = 0.0)
    {
        std::string edgeLabel = MakeEdgeLabel(limitStats, streamId);
        if (writerBlocked >= MinVisibleBlockedTimeShare) {
            edgeLabel += Format("%vwriter{blocked_share=%.2f}", edgeLabel.empty() ? "" : ", ", writerBlocked);
        }
        std::string arrow = edgeLabel.empty()
            ? "-->"
            : "-->|" + MermaidSafeEdgeLabel(edgeLabel) + "|";
        Out.AppendFormat("  %v %v %v\n", fromNode, arrow, toNode);

        int idx = EdgeIndex++;
        if (GetMaxFillRateForStream(limitStats, streamId) >= WarningFillRateThreshold ||
            GetMaxBlockedTimeShareForStream(limitStats, streamId) >= WarningBlockedTimeShareThreshold ||
            writerBlocked >= WarningBlockedTimeShareThreshold)
        {
            WarningEdgeIndices.push_back(idx);
        }
        return idx;
    }

    void EmitReadDelayEdge(const TReadDelayEdge& edge)
    {
        std::string blockerNode = "s_" + MermaidSafeId(edge.BlockerStreamGraphEntityId.Underlying());
        std::string delayedNode = "s_" + MermaidSafeId(edge.DelayedStreamGraphEntityId.Underlying());
        std::string delayStr = MermaidSafeEdgeLabel(Format("delay=%v", edge.Delay));
        Out.AppendFormat("  %v -->|%v| %v\n", blockerNode, delayStr, delayedNode);
        ReadDelayEdgeIndices.push_back(EdgeIndex++);
    }

    void EmitWarningNodeStyle(const std::string& nodeId)
    {
        Out.AppendFormat("  style %v %v\n", nodeId, WarningNodeStyle);
    }

    void EmitLinkStyles()
    {
        for (int idx : WarningEdgeIndices) {
            Out.AppendFormat("  linkStyle %v %v\n", idx, WarningLinkStyle);
        }
        for (int idx : ReadDelayEdgeIndices) {
            Out.AppendFormat("  linkStyle %v %v\n", idx, ReadDelayLinkStyle);
        }
    }

    void EmitClickLink(const std::string& nodeId, const std::string& url)
    {
        if (url.empty()) {
            return;
        }
        Out.AppendFormat("  click %v href \"%v\" _blank\n", nodeId, url);
    }
};

////////////////////////////////////////////////////////////////////////////////

constexpr auto& YtUrlPrefix = NInternalUrls::YtUrlPrefix;

// Returns the computation URL for clickable links, or empty string if options lack cluster/path.
std::string MakeComputationUrl(const TDrawPipelineGraphOptions& options, const TComputationId& computationId)
{
    if (YtUrlPrefix.empty() || options.ClusterName.empty() || options.PipelinePath.empty()) {
        return {};
    }
    return Format("%v%v/flows/computations/%v/details?path=%v",
        YtUrlPrefix,
        options.ClusterName,
        computationId,
        options.PipelinePath);
}

// The computation's graph entity id, used as the mermaid node identity.
TGraphEntityId ComputationGraphId(const TComputationId& computationId, const TPipelineComputationDescription& computation)
{
    return computation.Id.Underlying().empty() ? MakeComputationGraphId(computationId) : computation.Id;
}

// Iterate (outStreamId, inStreamId) pairs from StreamsDependency in sorted order.
// Calls callback(outStreamId, inStreamId) for each pair.
template <class TCallback>
void ForEachDependencyPair(
    const THashMap<TGraphEntityId, THashSet<TGraphEntityId>>& streamsDependency,
    TCallback&& callback)
{
    for (const auto& outStreamId : SortedKeys(streamsDependency)) {
        for (const auto& inStreamId : SortedElements(streamsDependency.at(outStreamId))) {
            callback(outStreamId, inStreamId);
        }
    }
}

std::string MermaidDirection(EGraphOrientation orientation)
{
    return orientation == EGraphOrientation::Horizontal ? "LR" : "TD";
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::string BuildMermaidComputationsGraph(
    const TPipelineDescription& pipeline,
    const TDrawPipelineGraphOptions& options)
{
    auto writerBlockedShares = ComputeWriterBlockedTimeShares(pipeline);

    TMermaidBuilder b;
    b.Out.AppendFormat("flowchart %v\n", MermaidDirection(options.Orientation));

    // Stream nodes.
    b.Out.AppendString("  %% Streams\n");
    for (const auto& streamId : SortedKeys(pipeline.Streams)) {
        b.EmitStreamNode(pipeline.Streams.at(streamId));
    }

    // Computation nodes.
    b.Out.AppendString("  %% Computations\n");
    auto sortedComputationIds = SortedKeys(pipeline.Computations);
    for (const auto& computationId : sortedComputationIds) {
        const auto& computation = pipeline.Computations.at(computationId);
        b.EmitComputationNode("c_" + MermaidSafeId(ComputationGraphId(computationId, computation).Underlying()), computation);
    }

    // Edges.
    b.Out.AppendString("  %% Edges\n");
    for (const auto& computationId : sortedComputationIds) {
        const auto& computation = pipeline.Computations.at(computationId);
        std::string cNode = "c_" + MermaidSafeId(ComputationGraphId(computationId, computation).Underlying());

        // Input edges (regular + source streams) → computation.
        std::vector<TGraphEntityId> inputStreams(computation.InputStreams.begin(), computation.InputStreams.end());
        inputStreams.insert(inputStreams.end(), computation.SourceStreams.begin(), computation.SourceStreams.end());
        std::sort(inputStreams.begin(), inputStreams.end());
        for (const auto& streamId : inputStreams) {
            b.EmitEdge("s_" + MermaidSafeId(streamId.Underlying()), cNode, computation.InputLimitStats, streamId, GetOrDefault(writerBlockedShares, streamId, TWriterBlockedShare{}).Share);
        }

        // Output edges: computation → stream.
        for (const auto& streamId : SortedElements(computation.OutputStreams)) {
            b.EmitEdge(cNode, "s_" + MermaidSafeId(streamId.Underlying()), computation.OutputLimitStats, streamId);
        }
    }

    // Read-delay edges.
    b.Out.AppendString("  %% Read-delay edges\n");
    for (const auto& edge : pipeline.ReadDelayEdges) {
        b.EmitReadDelayEdge(edge);
    }

    // Styles.
    b.Out.AppendString("  %% Styles\n");
    for (const auto& computationId : sortedComputationIds) {
        const auto& computation = pipeline.Computations.at(computationId);
        if (IsComputationWarning(computation)) {
            b.EmitWarningNodeStyle("c_" + MermaidSafeId(ComputationGraphId(computationId, computation).Underlying()));
        }
    }
    b.EmitLinkStyles();

    // Clickable links.
    if (!options.ClusterName.empty() && !options.PipelinePath.empty()) {
        b.Out.AppendString("  %% Links\n");
        for (const auto& computationId : sortedComputationIds) {
            const auto& computation = pipeline.Computations.at(computationId);
            b.EmitClickLink(
                "c_" + MermaidSafeId(ComputationGraphId(computationId, computation).Underlying()),
                MakeComputationUrl(options, computationId));
        }
    }

    return b.Out.Flush();
}

////////////////////////////////////////////////////////////////////////////////

std::string BuildMermaidUnrolledGraph(
    const TPipelineDescription& pipeline,
    const TDrawPipelineGraphOptions& options)
{
    auto writerBlockedShares = ComputeWriterBlockedTimeShares(pipeline);

    TMermaidBuilder b;
    b.Out.AppendFormat("flowchart %v\n", MermaidDirection(options.Orientation));

    auto sortedComputationIds = SortedKeys(pipeline.Computations);

    // Stream nodes.
    b.Out.AppendString("  %% Streams\n");
    for (const auto& streamId : SortedKeys(pipeline.Streams)) {
        b.EmitStreamNode(pipeline.Streams.at(streamId));
    }

    // Computation sub-nodes.
    // - Non-empty StreamsDependency: one sub-node per (output, input) pair.
    // - Empty StreamsDependency: single node (source/sink).
    b.Out.AppendString("  %% Computation sub-nodes\n");
    std::vector<std::pair<std::string, bool>> subNodes; // (nodeId, isWarning)

    for (const auto& computationId : sortedComputationIds) {
        const auto& computation = pipeline.Computations.at(computationId);
        std::string baseNode = "c_" + MermaidSafeId(ComputationGraphId(computationId, computation).Underlying());
        const bool isWarning = IsComputationWarning(computation);

        if (computation.StreamsDependency.empty()) {
            b.EmitComputationNode(baseNode, computation);
            subNodes.emplace_back(baseNode, isWarning);
        } else {
            ForEachDependencyPair(computation.StreamsDependency, [&] (const auto& outId, const auto& inId) {
                std::string nodeId = baseNode + "__i_" + MermaidSafeId(inId.Underlying()) + "__o_" + MermaidSafeId(outId.Underlying());
                b.EmitComputationNode(nodeId, computation);
                subNodes.emplace_back(nodeId, isWarning);
            });
        }
    }

    // Edges.
    b.Out.AppendString("  %% Edges\n");
    for (const auto& computationId : sortedComputationIds) {
        const auto& computation = pipeline.Computations.at(computationId);
        std::string baseNode = "c_" + MermaidSafeId(ComputationGraphId(computationId, computation).Underlying());

        if (computation.StreamsDependency.empty()) {
            // Single node: all inputs/sources → node → all outputs.
            std::vector<TGraphEntityId> inputStreams(computation.InputStreams.begin(), computation.InputStreams.end());
            inputStreams.insert(inputStreams.end(), computation.SourceStreams.begin(), computation.SourceStreams.end());
            std::sort(inputStreams.begin(), inputStreams.end());
            for (const auto& streamId : inputStreams) {
                b.EmitEdge("s_" + MermaidSafeId(streamId.Underlying()), baseNode, computation.InputLimitStats, streamId, GetOrDefault(writerBlockedShares, streamId, TWriterBlockedShare{}).Share);
            }
            for (const auto& streamId : SortedElements(computation.OutputStreams)) {
                b.EmitEdge(baseNode, "s_" + MermaidSafeId(streamId.Underlying()), computation.OutputLimitStats, streamId);
            }
        } else {
            // Unrolled: input_stream → sub-node → output_stream.
            ForEachDependencyPair(computation.StreamsDependency, [&] (const auto& outId, const auto& inId) {
                std::string cNode = baseNode + "__i_" + MermaidSafeId(inId.Underlying()) + "__o_" + MermaidSafeId(outId.Underlying());
                b.EmitEdge("s_" + MermaidSafeId(inId.Underlying()), cNode, computation.InputLimitStats, inId, GetOrDefault(writerBlockedShares, inId, TWriterBlockedShare{}).Share);
                b.EmitEdge(cNode, "s_" + MermaidSafeId(outId.Underlying()), computation.OutputLimitStats, outId);
            });
        }
    }

    // Styles.
    b.Out.AppendString("  %% Styles\n");
    for (const auto& [nodeId, isWarning] : subNodes) {
        if (isWarning) {
            b.EmitWarningNodeStyle(nodeId);
        }
    }
    b.EmitLinkStyles();

    // Clickable links.
    if (!options.ClusterName.empty() && !options.PipelinePath.empty()) {
        b.Out.AppendString("  %% Links\n");
        for (const auto& computationId : sortedComputationIds) {
            const auto& computation = pipeline.Computations.at(computationId);
            std::string baseNode = "c_" + MermaidSafeId(ComputationGraphId(computationId, computation).Underlying());
            std::string url = MakeComputationUrl(options, computationId);

            if (computation.StreamsDependency.empty()) {
                b.EmitClickLink(baseNode, url);
            } else {
                ForEachDependencyPair(computation.StreamsDependency, [&] (const auto& outId, const auto& inId) {
                    std::string nodeId = baseNode + "__i_" + MermaidSafeId(inId.Underlying()) + "__o_" + MermaidSafeId(outId.Underlying());
                    b.EmitClickLink(nodeId, url);
                });
            }
        }
    }

    return b.Out.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
