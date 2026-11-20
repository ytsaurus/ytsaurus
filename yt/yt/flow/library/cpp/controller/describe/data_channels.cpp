#include "data_channels.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/state.h>

#include <library/cpp/yt/string/format.h>

#include <algorithm>
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
    // The writer's aliases in its own order, to pick the label of an edge.
    THashMap<TGraphEntityId, std::vector<std::string>> writerAliasLists;

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

    auto addWriterChannels = [&] (const std::vector<TDataChannel>& channels, const TGraphEntityId& writerId) {
        addChannels(channels, EDataChannelAccess::Write, writerId, aliasWriters);
        auto& writerAliases = writerAliasLists[writerId];
        for (const auto& channel : channels) {
            if (channel.Access == EDataChannelAccess::Write) {
                writerAliases.insert(writerAliases.end(), channel.Aliases.begin(), channel.Aliases.end());
            }
        }
    };

    // (from, to) -> matched aliases; a key-visitor stream visiting its own computation's state matches no alias.
    std::map<std::pair<TGraphEntityId, TGraphEntityId>, std::set<std::string>> edgeAliases;

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
            // A sink without input streams is not in the graph.
            auto globalSinkId = MakeGlobalSinkId(computationId, localSinkId);
            if (!pipeline.Sinks.contains(globalSinkId)) {
                continue;
            }

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
            addWriterChannels(channels, MakeSinkGraphId(globalSinkId));
        }

        auto computationGraphId = MakeComputationGraphId(computationId);
        auto stateManagerNames = GetKeys(computationSpec->ExternalStateManagers);
        std::sort(stateManagerNames.begin(), stateManagerNames.end());
        for (const auto& name : stateManagerNames) {
            auto channels = getChannels("external_state_manager", computationId, name, [&] {
                const auto& stateManagerSpec = GetOrCrash(computationSpec->ExternalStateManagers, name);
                auto traits = registry->CreateExternalStateManagerDescribeTraits(stateManagerSpec->ExternalStateManagerClassName, describeTraitsContext);
                if (!traits) {
                    return std::vector<TDataChannel>();
                }

                return traits->GetDataChannels(registry->ParseExternalStateManagerParameters(stateManagerSpec), /*states*/ {});
            });
            addWriterChannels(channels, computationGraphId);
        }

        for (const auto& [localStreamId, streamSpec] : computationSpec->KeyVisitorStreams) {
            auto streamGraphId = MakeStreamGraphId(MakeGlobalStreamId(computationId, localStreamId, computationSpec));

            // Null names and external names mean the scan-all mode: all internal states and external state managers.
            bool visitsOwnState = (!streamSpec->Names && !streamSpec->ExternalNames) ||
                (streamSpec->Names && !streamSpec->Names->empty());
            auto addVisitedNames = [&] (const std::optional<THashSet<std::string>>& names) {
                if (!names) {
                    return;
                }

                for (const auto& name : *names) {
                    if (computationSpec->ExternalStateManagers.contains(name)) {
                        visitsOwnState = true;
                        continue;
                    }

                    const auto* joinerSpec = computationSpec->ExternalStateJoiners.FindPtr(name);
                    // A key-visitor stream does not sweep a joiner that is not visitor-driven.
                    if (!joinerSpec || !registry->IsExternalStateJoinerVisitorDriven((*joinerSpec)->ExternalStateJoinerClassName)) {
                        continue;
                    }

                    auto channels = getChannels("external_state_joiner", computationId, name, [&] {
                        auto traits = registry->CreateExternalStateJoinerDescribeTraits((*joinerSpec)->ExternalStateJoinerClassName, describeTraitsContext);
                        if (!traits) {
                            return std::vector<TDataChannel>();
                        }

                        return traits->GetDataChannels(registry->ParseExternalStateJoinerParameters(*joinerSpec), /*states*/ {});
                    });
                    addChannels(channels, EDataChannelAccess::Read, streamGraphId, aliasReaders);
                }
            };

            addVisitedNames(streamSpec->ExternalNames);
            addVisitedNames(streamSpec->WeakExternalNames);

            if (visitsOwnState) {
                edgeAliases[std::pair(computationGraphId, streamGraphId)];
            }
        }
    }

    for (const auto& [alias, writers] : aliasWriters) {
        const auto* readers = aliasReaders.FindPtr(alias);
        if (!readers) {
            continue;
        }

        for (const auto& writer : writers) {
            for (const auto& reader : *readers) {
                edgeAliases[std::pair(writer, reader)].insert(alias);
            }
        }
    }

    for (const auto& [ids, aliases] : edgeAliases) {
        const auto& [fromGraphId, toGraphId] = ids;
        // The writer's first matched alias is the one to display.
        std::string label;
        if (!aliases.empty()) {
            const auto& writerAliases = GetOrCrash(writerAliasLists, fromGraphId);
            label = *std::find_if(writerAliases.begin(), writerAliases.end(), [&] (const auto& alias) {
                return aliases.contains(alias);
            });
        }

        YT_TLOG_DEBUG("Data channel edge found")
            .With("FromGraphEntityId", fromGraphId)
            .With("ToGraphEntityId", toGraphId)
            .With("Label", label)
            .With("MatchedAliases", aliases);

        auto& edge = pipeline.Edges.emplace_back();
        edge.From = fromGraphId;
        edge.To = toGraphId;
        edge.Kind = EGraphEdgeKind::DataChannel;
        edge.Label = std::move(label);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
