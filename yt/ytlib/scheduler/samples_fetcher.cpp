#include "stdafx.h"
#include "samples_fetcher.h"
#include "private.h"
#include "config.h"

#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/rpc/channel_cache.h>

namespace NYT {
namespace NScheduler {

using namespace NChunkHolder;
using namespace NTableClient::NProto;

static NRpc::TChannelCache ChannelCache;

////////////////////////////////////////////////////////////////////

TSamplesFetcher::TSamplesFetcher(
    TSchedulerConfigPtr config,
    IInvoker::TPtr invoker,
    const TOperationId& operationId)
    : Config(config)
    , Invoker(invoker)
    , Logger(SchedulerLogger)
    , Promise(NewPromise< TValueOrError<void> >())
{
    Logger.AddTag(Sprintf("OperationId: %s", ~operationId.ToString()));
}

void TSamplesFetcher::AddChunk(const TInputChunk& chunk)
{
    Chunks.push_back(chunk);
}

const std::vector<TKeySample>& TSamplesFetcher::GetSamples() const
{
    return Samples;
}

TFuture< TValueOrError<void> > TSamplesFetcher::Run()
{
    SendRequests();
    return Promise;
}

void TSamplesFetcher::SendRequests()
{
    // Construct address -> chunk* map.
    typedef yhash_map<Stroka, std::vector<TInputChunk*> > TAddressToChunks;
    TAddressToChunks addressToChunks;

    FOREACH (auto chunk, UnfetchedChunks) {
        auto chunkId = TChunkId::FromProto(chunk->slice().chunk_id());
        bool chunkAvailable = false;
        FOREACH (const auto& address, chunk->holder_addresses()) {
            if (DeadNodes.find(address) == DeadNodes.end() &&
                DeadChunks.find(std::make_pair(address, chunkId)) == DeadChunks.end())
            {
                addressToChunks[address].push_back(chunk);
                chunkAvailable = true;
            }
        }
        if (!chunkAvailable) {
            Promise.Set(TError("Unable to fetch table samples for chunk %s from any of nodes [%s]",
                ~chunkId.ToString(),
                ~JoinToString(chunk->holder_addresses())));
            return;
        }
    }

    LOG_INFO("Fetching samples for %" PRISZT " chunks from up to %" PRISZT " nodes",
        UnfetchedChunks.size(),
        addressToChunks.size());

    // Sort nodes by number of chunks (in decreasing order).
    std::vector<TAddressToChunks::iterator> addressIts;
    for (auto it = addressToChunks.begin(); it != addressToChunks.end(); ++it) {
        addressIts.push_back(it);
    }
    std::sort(
        addressIts.begin(),
        addressIts.end(),
        [=] (const TAddressToChunks::iterator& lhs, const TAddressToChunks::iterator& rhs) {
            return lhs->second.size() > rhs->second.size();
        });

    // Pick nodes greedily.
    auto awaiter = New<TParallelAwaiter>(Invoker);
    yhash_set<TInputChunk*> requestedChunks;
    FOREACH (const auto& it, addressIts) {
        auto address = it->first;
        auto channel = ChannelCache.GetChannel(address);
        TChunkHolderServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Config->NodeRpcTimeout);

        // Take all (still unfetched) chunks from this node.
        auto req = proxy.GetTableSamples();
        std::vector<TInputChunk*> chunksToRequest;
        FOREACH (auto chunk, it->second) {
            if (requestedChunks.find(chunk) == requestedChunks.end()) {
                chunksToRequest.push_back(chunk);
                YVERIFY(requestedChunks.insert(chunk).second);
            }
        }

        // Send the request, if not empty.
        if (!chunksToRequest.empty()) {
            LOG_INFO("Requesting samples for %d chunks from %s",
                req->chunk_ids_size(),
                ~address);
            awaiter->Await(
                req->Invoke(),
                // TODO(babenko): cannot use Passed, investigate
                BIND(&TSamplesFetcher::OnResponse, MakeStrong(this), address, chunksToRequest));
        }
    }
    awaiter->Complete(BIND(&TSamplesFetcher::OnEndRound, MakeStrong(this)));
    LOG_INFO("%d requests sent", awaiter->GetRequestCount());
}

void TSamplesFetcher::OnResponse(
    const Stroka& address,
    std::vector<TInputChunk*> chunks,
    TChunkHolderServiceProxy::TRspGetTableSamples::TPtr rsp)
{
    if (rsp->IsOK()) {
        YASSERT(chunks.size() == rsp->samples_size());
        int samplesAdded = 0;
        for (int index = 0; index < static_cast<int>(chunks.size()); ++index) {
            auto* chunk = chunks[index];
            auto chunkId = TChunkId::FromProto(chunk->slice().chunk_id());
            const auto& chunkSamples = rsp->samples(index);
            if (chunkSamples.has_error()) {
                LOG_WARNING("Unable to fetch samples for chunk %s from %s\n%s",
                    ~chunkId.ToString(),
                    ~address,
                    ~TError::FromProto(chunkSamples.error()).ToString());
                YVERIFY(DeadChunks.insert(std::make_pair(address, chunkId)).second);
            } else {
                LOG_TRACE("Received %d samples for chunk %s",
                    chunkSamples.samples_size(),
                    ~chunkId.ToString());
                FOREACH (const auto& keySamples, chunkSamples.samples()) {
                    Samples.push_back(keySamples);
                    ++samplesAdded;
                }
                YVERIFY(UnfetchedChunks.erase(chunk) == 1);
            }
        }
        LOG_INFO("Received %d samples from %s",
            samplesAdded,
            ~address);
    } else {
        LOG_INFO("Error requesting samples from %s\n%s",
            ~address,
            ~rsp->GetError().ToString());
        YVERIFY(DeadNodes.insert(address).second);
    }
}

void TSamplesFetcher::OnEndRound()
{
    if (UnfetchedChunks.empty()) {
        Promise.Set(TError());
    } else {
        SendRequests();
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

