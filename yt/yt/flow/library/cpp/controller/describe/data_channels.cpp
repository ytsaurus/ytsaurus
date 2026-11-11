#include "data_channels.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/state.h>

#include <library/cpp/yt/string/format.h>

#include <map>
#include <set>

namespace NYT::NFlow::NDescribe {

////////////////////////////////////////////////////////////////////////////////

namespace {

TEntityControllerStateMap GetEntityControllerStates(
    const TJobManagerStatePtr& jobManagerState,
    const TComputationId& computationId,
    const std::string& prefix)
{
    TEntityControllerStateMap result;
    if (!jobManagerState) {
        return result;
    }

    const auto* computationStates = jobManagerState->Computations.FindPtr(computationId);
    if (!computationStates) {
        return result;
    }

    for (const auto& [name, state] : *computationStates) {
        if (name.starts_with(prefix)) {
            result.emplace(name.substr(prefix.size()), state);
        }
    }

    return result;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void FillDataChannelEdges(
    const TPipelineSpecPtr& pipelineSpec,
    const TParsedPipelineParameters& parsedParameters,
    const TJobManagerStatePtr& jobManagerState,
    const TDescribeTraitsContext& describeTraitsContext,
    const NLogging::TLogger& logger,
    TPipelineDescription& pipeline)
{
    const auto& Logger = logger;
    auto* registry = TRegistry::Get();

    auto getChannels = [&] (
        TStringBuf kind,
        const TComputationId& computationId,
        TStringBuf name,
        const std::function<std::vector<TDataChannel>()>& compute) -> std::vector<TDataChannel> {
        // Describe must never fail because of one entity; parse errors are reported by ValidateSpecs.
        try {
            return compute();
        } catch (const std::exception& ex) {
            YT_TLOG_DEBUG("Failed to get data channels")
                .With("ComputationId", computationId)
                .With("Kind", kind)
                .With("Name", name)
                .With(ex);

            return {};
        }
    };

    THashMap<std::string, std::vector<TGraphEntityId>> aliasWriters;
    THashMap<std::string, std::vector<TGraphEntityId>> aliasReaders;
    THashMap<TGraphEntityId, std::vector<std::string>> sinkAliasLists;

    auto addChannels = [] (
        const std::vector<TDataChannel>& channels,
        EDataChannelAccess access,
        const TGraphEntityId& entityId,
        THashMap<std::string, std::vector<TGraphEntityId>>& index) {
        for (const auto& channel : channels) {
            if (channel.Access != access) {
                continue;
            }

            for (const auto& alias : channel.Aliases) {
                index[alias].push_back(entityId);
            }
        }
    };

    for (const auto& [computationId, computationSpec] : pipelineSpec->Computations) {
        for (const auto& [localStreamId, sourceSpec] : computationSpec->SourceStreams) {
            auto channels = getChannels("source_stream", computationId, localStreamId.Underlying(), [&] {
                auto traits = registry->CreateSourceDescribeTraits(sourceSpec->SourceClassName, describeTraitsContext);
                const auto* parameters = parsedParameters.Sources.FindPtr(std::pair(computationId, localStreamId));
                if (!traits || !parameters) {
                    return std::vector<TDataChannel>();
                }

                return traits->GetDataChannels(
                    *parameters,
                    GetEntityControllerStates(jobManagerState, computationId, Format("/%v/%v/", SourcesStateNameSegment, localStreamId)));
            });
            auto globalStreamId = MakeGlobalStreamId(computationId, localStreamId, computationSpec);
            addChannels(channels, EDataChannelAccess::Read, MakeSourceGraphId(globalStreamId), aliasReaders);
        }

        for (const auto& [localSinkId, sinkSpec] : computationSpec->Sinks) {
            auto channels = getChannels("sink", computationId, localSinkId.Underlying(), [&] {
                auto traits = registry->CreateSinkDescribeTraits(sinkSpec->SinkClassName, describeTraitsContext);
                const auto* parameters = parsedParameters.Sinks.FindPtr(std::pair(computationId, localSinkId));
                if (!traits || !parameters) {
                    return std::vector<TDataChannel>();
                }

                return traits->GetDataChannels(
                    *parameters,
                    GetEntityControllerStates(jobManagerState, computationId, Format("/%v/%v/", SinksStateNameSegment, localSinkId)));
            });
            auto sinkGraphId = MakeSinkGraphId(MakeGlobalSinkId(computationId, localSinkId));
            addChannels(channels, EDataChannelAccess::Write, sinkGraphId, aliasWriters);
            auto& sinkAliases = sinkAliasLists[sinkGraphId];
            for (const auto& channel : channels) {
                if (channel.Access == EDataChannelAccess::Write) {
                    sinkAliases.insert(sinkAliases.end(), channel.Aliases.begin(), channel.Aliases.end());
                }
            }
        }
    }

    THashSet<TGraphEntityId> sinkGraphIds;
    for (const auto& [sinkId, sink] : pipeline.Sinks) {
        sinkGraphIds.insert(sink.Id);
    }

    THashSet<TGraphEntityId> sourceGraphIds;
    for (const auto& [streamId, source] : pipeline.Sources) {
        sourceGraphIds.insert(source.Id);
    }

    std::map<std::pair<TGraphEntityId, TGraphEntityId>, std::set<std::string>> edgeAliases;
    for (const auto& [alias, writers] : aliasWriters) {
        const auto* readers = aliasReaders.FindPtr(alias);
        if (!readers) {
            continue;
        }

        for (const auto& writer : writers) {
            if (!sinkGraphIds.contains(writer)) {
                continue;
            }

            for (const auto& reader : *readers) {
                if (!sourceGraphIds.contains(reader)) {
                    continue;
                }

                edgeAliases[std::pair(writer, reader)].insert(alias);
            }
        }
    }

    for (const auto& [ids, aliases] : edgeAliases) {
        const auto& [sinkGraphId, sourceGraphId] = ids;
        // The sink's first matched alias is the one to display.
        const auto& sinkAliases = GetOrCrash(sinkAliasLists, sinkGraphId);
        const auto& label = *std::find_if(sinkAliases.begin(), sinkAliases.end(), [&] (const auto& alias) {
            return aliases.contains(alias);
        });

        YT_TLOG_DEBUG("Data channel edge found")
            .With("SinkGraphEntityId", sinkGraphId)
            .With("SourceGraphEntityId", sourceGraphId)
            .With("Label", label)
            .With("MatchedAliases", aliases);

        auto& edge = pipeline.Edges.emplace_back();
        edge.From = sinkGraphId;
        edge.To = sourceGraphId;
        edge.Kind = EGraphEdgeKind::DataChannel;
        edge.Label = label;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
