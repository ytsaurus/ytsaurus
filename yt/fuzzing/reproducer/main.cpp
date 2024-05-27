#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <google/protobuf/text_format.h>
#include <library/cpp/getopt/small/last_getopt_parse_result.h>
#include <library/cpp/resource/resource.h>
#include <yt/fuzzing/lib/fuzzing.h>
#include <yt/fuzzing/lib/timer.h>
#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/server/node/cluster_node/program.h>
#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

NYT::NRpc::IServerPtr server;

std::unique_ptr<NYT::NClusterNode::TClusterNodeProgram> DataNode;
static const auto& Logger = NYT::NClusterNode::ClusterNodeLogger;

void Init() {
  DataNode = std::make_unique<NYT::NClusterNode::TClusterNodeProgram>();

  const char* envYtRepoPath = std::getenv("YT_REPO_PATH");
  if (!envYtRepoPath || envYtRepoPath[0] == '\0') {
    std::cerr << "Environment variable YT_REPO_PATH is not set or empty." << std::endl;
    exit(1);
  }

  std::string ytRepoPath = envYtRepoPath;
  std::thread serverThread([ytRepoPath]() {
    const std::string configPath = ytRepoPath + "/yt/fuzzing/datanode/node.yson";

    int argc = 3;
    const char* argv[] = {"data-node", "--config", configPath.c_str(), nullptr};
    std::cout << "Starting datanode ..." << std::endl;
    DataNode->Run(argc, argv);
  });
  serverThread.detach();
  std::this_thread::sleep_for(std::chrono::seconds(10));
}

template <typename TRequest, typename TProxyMethod>
void SendRequest(const std::string& methodName, const TRequest& request, TProxyMethod proxyMethod,
                 const NProtoBuf::RepeatedPtrField<TBasicString<char>>& attachments) {
  const auto before_rss = fuzzing::getCurrentRSS();
  YT_LOG_INFO("[FUZZER] Sending %v, attachments size: %v", methodName, attachments.size());
  std::cerr << "[FUZZER] Sending " << methodName << ", attachments size: " << attachments.size()
            << std::endl;
  std::string rspMsg = "";
  size_t retry = 0;
  do {
    if (retry) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    fuzzing::Timer t;
    server = DataNode->WaitRpcServer();
    auto channel = NYT::NRpc::CreateLocalChannel(server);
    NYT::NChunkClient::TDataNodeServiceProxy proxy(channel);

    auto req = (proxy.*proxyMethod)();
    req->CopyFrom(request);
    req->Attachments().reserve(attachments.size());
    for (const auto& attachment : attachments) {
      req->Attachments().push_back(NYT::TSharedRef::FromString(TString(attachment)));
    }
    auto rspOrError = NYT::NConcurrency::WaitFor(req->Invoke());
    // YT_LOG_INFO("[FUZZER] %v took %v ms, response: %v", methodName, t.Reset(),
    // rspOrError.GetMessage());
    rspMsg = rspOrError.GetMessage();
    const auto after_rss = fuzzing::getCurrentRSS();
    std::cerr << methodName << " took " << t.Reset() << " ms, response: " << rspMsg
              << ", retry=" << retry << ", attachments size:" << attachments.size()
              << ", real attach size: " << req->Attachments().size()
              << ", before rss=" << before_rss << ", after_rss=" << after_rss
              << ", diff=" << (after_rss - before_rss) * 1.0 / (1024 * 1024 * 1024) << "GB"
              << std::endl;

    ++retry;
  } while (rspMsg == "Master is not connected");
}

int main(int argc, char* argv[]) {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  if (argc != 2) {
    std::cerr << "Usage: " << argv[0] << " FILE_NAME" << std::endl;
    return -1;
  }

  std::fstream str_input(argv[1], std::ios::in | std::ios::binary);
  if (!str_input) {
    std::cerr << "Failed to open file: " << argv[1] << std::endl;
    return -1;
  }

  NYT::NChunkClient::NProto::TDatanodeFuzzerInput parsed_input;
  if (!parsed_input.ParseFromIstream(&str_input)) {
    std::cerr << "Failed to parse protobuf message." << std::endl;
    return -1;
  }
  std::cout << parsed_input.DebugString() << std::endl;
  Init();

  for (const auto& request_with_attachments : parsed_input.requests()) {
    const auto& request = request_with_attachments.request();
    const auto& attachments = request_with_attachments.attachments();
    switch (request.request_case()) {
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kStartChunk:
        SendRequest("StartChunk", request.start_chunk(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::StartChunk, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kFinishChunk:
        SendRequest("FinishChunk", request.finish_chunk(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::FinishChunk, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kCancelChunk:
        SendRequest("CancelChunk", request.cancel_chunk(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::CancelChunk, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kPingSession:
        SendRequest("PingSession", request.ping_session(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::PingSession, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kPutBlocks:
        SendRequest("PutBlocks", request.put_blocks(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::PutBlocks, attachments);
        break;
      // case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kSendBlocks:
      //     SendRequest("SendBlocks", request.send_blocks(),
      //     &NYT::NChunkClient::TDataNodeServiceProxy::SendBlocks, attachments); break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kFlushBlocks:
        SendRequest("FlushBlocks", request.flush_blocks(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::FlushBlocks, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kUpdateP2PBlocks:
        SendRequest("UpdateP2PBlocks", request.update_p2p_blocks(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::UpdateP2PBlocks, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kProbeChunkSet:
        SendRequest("ProbeChunkSet", request.probe_chunk_set(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::ProbeChunkSet, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kProbeBlockSet:
        SendRequest("ProbeBlockSet", request.probe_block_set(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::ProbeBlockSet, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kGetBlockSet:
        SendRequest("GetBlockSet", request.get_block_set(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::GetBlockSet, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kGetBlockRange:
        SendRequest("GetBlockRange", request.get_block_range(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::GetBlockRange, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kGetChunkFragmentSet:
        SendRequest("GetChunkFragmentSet", request.get_chunk_fragment_set(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkFragmentSet, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kLookupRows:
        SendRequest("LookupRows", request.lookup_rows(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::LookupRows, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kGetChunkMeta:
        SendRequest("GetChunkMeta", request.get_chunk_meta(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkMeta, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kGetChunkSliceDataWeights:
        SendRequest("GetChunkSliceDataWeights", request.get_chunk_slice_data_weights(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkSliceDataWeights,
                    attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kGetChunkSlices:
        SendRequest("GetChunkSlices", request.get_chunk_slices(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::GetChunkSlices, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kGetTableSamples:
        SendRequest("GetTableSamples", request.get_table_samples(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::GetTableSamples, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kGetColumnarStatistics:
        SendRequest("GetColumnarStatistics", request.get_columnar_statistics(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::GetColumnarStatistics, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kDisableChunkLocations:
        SendRequest("DisableChunkLocations", request.disable_chunk_locations(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::DisableChunkLocations, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kDestroyChunkLocations:
        SendRequest("DestroyChunkLocations", request.destroy_chunk_locations(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::DestroyChunkLocations, attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kResurrectChunkLocations:
        SendRequest("ResurrectChunkLocations", request.resurrect_chunk_locations(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::ResurrectChunkLocations,
                    attachments);
        break;
      case NYT::NChunkClient::NProto::TFuzzerDatanodeSingleRequest::kAnnounceChunkReplicas:
        SendRequest("AnnounceChunkReplicas", request.announce_chunk_replicas(),
                    &NYT::NChunkClient::TDataNodeServiceProxy::AnnounceChunkReplicas, attachments);
        break;
      default:
        break;
    }
  }

  return 0;
}
