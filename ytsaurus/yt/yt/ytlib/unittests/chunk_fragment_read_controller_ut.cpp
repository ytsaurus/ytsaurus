#include <gtest/gtest.h>

#include <yt/yt/ytlib/chunk_client/chunk_fragment_read_controller.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/misc/numeric_helpers.h>

#include <random>

namespace NYT::NChunkClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(ErasureChunkFragmentReadController, StressTest)
{
    constexpr int Iterations = 50000;
    constexpr int Blocks = 5;
    constexpr int MaxBlockSize = 36;
    constexpr int MaxRequests = 10;
    constexpr auto CodecId = NErasure::ECodec::IsaReedSolomon_6_3;

    const auto* codec = NErasure::GetCodec(CodecId);

    for (int iteration = 0; iteration < Iterations; ++iteration) {
        std::mt19937 rng(iteration);

        std::vector<TSharedRef> blocks;
        std::vector<std::vector<TSharedRef>> blockParts;

        for (int blockIndex = 0; blockIndex < Blocks; ++blockIndex) {
            int blockSize = rng() % MaxBlockSize + 1;
            TString block;
            for (int index = 0; index < blockSize; ++index) {
                block += static_cast<char>('a' + rng() % 26);
            }
            blocks.push_back(TSharedRef::FromString(block));

            std::vector<TSharedRef> parts;
            auto partSize = DivCeil<int>(block.size(), codec->GetDataPartCount());
            for (int partIndex = 0; partIndex < codec->GetDataPartCount(); ++partIndex) {
                TString part;
                for (int index = partIndex * partSize; index < (partIndex + 1) * partSize; ++index) {
                    if (index < std::ssize(block)) {
                        part += block[index];
                    } else {
                        part += '$';
                    }
                }
                parts.push_back(TSharedRef::FromString(std::move(part)));
            }

            auto parityParts = codec->Encode(parts);
            parts.insert(parts.end(), parityParts.begin(), parityParts.end());
            blockParts.push_back(std::move(parts));
        }

        std::vector<TFragmentRequest> requests;
        int requestCount = rng() % MaxRequests + 1;
        bool shortRequests = rng() % 2 == 0;
        for (int index = 0; index < requestCount; ++index) {
            int blockIndex = rng() % blocks.size();
            int blockSize = blocks[blockIndex].size();
            int start = rng() % blockSize;
            int end;
            if (shortRequests) {
                end = start;
                if (start + 1 < blockSize && rng() % 2 == 0) {
                    end++;
                }
            } else {
                end = rng() % blockSize;
            }
            if (start > end) {
                std::swap(start, end);
            }
            requests.push_back(TFragmentRequest{
                .Length = end - start + 1,
                .BlockOffset = start,
                .BlockSize = blockSize,
                .BlockIndex = blockIndex,
                .FragmentIndex = index,
            });
        }

        std::vector<TSharedRef> responses;
        responses.resize(requestCount);

        auto chunkId = TGuid::FromString("0-0-66-0");
        auto controller = CreateChunkFragmentReadController(chunkId, CodecId, &responses);

        TReplicasWithRevision replicas;
        replicas.Revision = NHydra::NullRevision;
        for (int index = 0; index < codec->GetTotalPartCount(); ++index) {
            replicas.Replicas.push_back(TChunkReplicaInfo{
                .ReplicaIndex = index
            });
        }
        controller->SetReplicas(replicas);

        for (int index = 0; index < requestCount; ++index) {
            controller->RegisterRequest(requests[index]);
        }

        auto* regularPlan = controller->TryMakePlan();
        ASSERT_TRUE(regularPlan);

        auto* repairPlan = controller->TryMakePlan();
        ASSERT_TRUE(repairPlan);

        auto processRequest = [&] (
            int peerIndex,
            const NChunkClient::NProto::TReqGetChunkFragmentSet_TSubrequest& request)
        {
            std::vector<TSharedRef> responses;
            for (const auto& subrequest : request.fragments()) {
                const auto& part = blockParts[subrequest.block_index()][peerIndex];
                responses.push_back(
                    part.Slice(
                        subrequest.block_offset(),
                        subrequest.block_offset() + subrequest.length()));
            }

            return responses;
        };

        NChunkClient::NProto::TRspGetChunkFragmentSet_TSubresponse response;
        response.set_has_complete_chunk(true);

        std::vector<std::vector<TSharedRef>> regularResponses;
        for (int index = 0; index < codec->GetDataPartCount(); ++index) {
            NChunkClient::NProto::TReqGetChunkFragmentSet_TSubrequest request;
            controller->PrepareRpcSubrequest(regularPlan, index, &request);
            regularResponses.push_back(processRequest(index, request));
        }

        std::vector<std::vector<TSharedRef>> repairResponses;
        for (int index = 0; index < codec->GetTotalPartCount(); ++index) {
            NChunkClient::NProto::TReqGetChunkFragmentSet_TSubrequest request;
            controller->PrepareRpcSubrequest(repairPlan, index, &request);
            repairResponses.push_back(processRequest(index, request));
        }

        // 0 -- only regular plan, 1 -- only repair plan, 2 -- both.
        int mode = rng() % 3;
        std::vector<std::pair<int, int>> responseOrder;
        if (mode == 0 || mode == 2) {
            for (int index = 0; index < codec->GetDataPartCount(); ++index) {
                responseOrder.emplace_back(index, 0);
            }
        }
        if (mode == 1 || mode == 2) {
            for (int index = 0; index < codec->GetTotalPartCount(); ++index) {
                responseOrder.emplace_back(index, 1);
            }
        }

        std::shuffle(responseOrder.begin(), responseOrder.end(), rng);

        for (auto responseIndex : responseOrder) {
            auto partIndex = responseIndex.first;
            if (responseIndex.second == 0) {
                controller->HandleRpcSubresponse(
                    regularPlan,
                    partIndex,
                    response,
                    regularResponses[partIndex]);
            } else {
                controller->HandleRpcSubresponse(
                    repairPlan,
                    partIndex,
                    response,
                    repairResponses[partIndex]);
            }
        }

        EXPECT_TRUE(controller->IsDone());
        for (int index = 0; index < requestCount; ++index) {
            const auto& request = requests[index];
            auto expected = blocks[request.BlockIndex].Slice(
                request.BlockOffset,
                request.BlockOffset + request.Length);

            EXPECT_EQ(ToString(expected), ToString(responses[index]));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkClient
