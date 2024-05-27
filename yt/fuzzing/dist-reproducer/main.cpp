#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>
#include <google/protobuf/text_format.h>
#include <library/cpp/getopt/small/last_getopt_parse_result.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/yt/logging/logger.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <yt/fuzzing/lib/fuzzing.h>
#include <yt/fuzzing/lib/timer.h>
#include <yt/yt/client/api/rpc_proxy/api_service_proxy.h>
#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/api/rpc_proxy/connection.h>
#include <yt/yt/client/api/rpc_proxy/connection_impl.h>
#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/server/master/cell_master/program.h>
#include <yt/yt/server/node/cluster_node/program.h>
#include <yt/yt/server/rpc_proxy/program.h>
#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>
#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <string>

namespace {

static const auto& Logger = NYT::NRpcProxy::RpcProxyLogger;
std::set<pid_t> childPIDs;
std::atomic_bool done = false;

bool Init() {
  const char* envYtRepoPath = std::getenv("YT_REPO_PATH");
  if (!envYtRepoPath || envYtRepoPath[0] == '\0') {
    std::cerr << "Environment variable YT_REPO_PATH is not set or empty." << std::endl;
    exit(1);
  }
  std::string ytRepoPath = envYtRepoPath;
  const char* envYtBuildPath = std::getenv("YT_BUILD_PATH");
  if (!envYtBuildPath || envYtBuildPath[0] == '\0') {
    std::cerr << "Environment variable YT_BUILD_PATH is not set or empty." << std::endl;
    exit(1);
  }
  std::string ytBuildPath = envYtBuildPath;

  auto forkAndExec = [](const std::string& programPath, const std::string& configPath) {
    pid_t pid = fork();
    if (pid == 0) {
      execl(programPath.c_str(), programPath.c_str(), "--config", configPath.c_str(),
            (char*)nullptr);
      std::cerr << "Failed to exec " << programPath << std::endl;
      exit(1);
    } else if (pid < 0) {
      std::cerr << "Fork failed for " << programPath << std::endl;
      exit(1);
    }
    return pid;
  };

  pid_t pid1 = forkAndExec(ytBuildPath + "/yt/yt/server/all/ytserver-master",
                           ytRepoPath + "/yt/fuzzing/distributed/master.yson");
  pid_t pid2 = forkAndExec(ytBuildPath + "/yt/yt/server/all/ytserver-node",
                           ytRepoPath + "/yt/fuzzing/distributed/node.yson");
  pid_t pid3 = forkAndExec(ytBuildPath + "/yt/yt/server/all/ytserver-proxy",
                           ytRepoPath + "/yt/fuzzing/distributed/rpc_proxy.yson");

  childPIDs = {pid1, pid2, pid3};
  auto monitorProcesses = [pid1, pid2, pid3]() {
    int status;
    while (!childPIDs.empty()) {
      pid_t pid = waitpid(-1, &status, 0);

      if (pid == -1) {
        std::cerr << "Error waiting for child process." << std::endl;
        break;
      } else if (pid > 0) {
        childPIDs.erase(pid);
        if (done) continue;
        if (WIFEXITED(status) || WIFSIGNALED(status)) {
          std::cerr << "Process " << pid << " exited. Terminating main program." << std::endl;
          kill(pid1, SIGKILL);
          kill(pid2, SIGKILL);
          kill(pid3, SIGKILL);
          exit(1);
        }
      }
    }
  };

  std::thread monitorThread(monitorProcesses);
  monitorThread.detach();

  std::this_thread::sleep_for(std::chrono::seconds(10));
  return true;
}

using namespace NYT;

auto CreateRpcClient() {
  auto connectionConfig = New<NApi::NRpcProxy::TConnectionConfig>();
  connectionConfig->ProxyAddresses = {"127.0.0.1:9013"};
  connectionConfig->ProxyListUpdatePeriod = TDuration::Seconds(5);
  auto connection = New<NYT::NApi::NRpcProxy::TConnection>(
      connectionConfig, NYT::NApi::NRpcProxy::TConnectionOptions{});
  return connection->CreateChannelByAddress("127.0.0.1:9013");
}

template <typename TRequest, typename TProxyMethod>
void SendRequest(const std::string& methodName, const TRequest& request, TProxyMethod proxyMethod,
                 const NProtoBuf::RepeatedPtrField<TBasicString<char>>& attachments) {
  const auto before_rss = fuzzing::getCurrentRSS();
  std::cerr << "[FUZZER] Sending " << methodName << ", attachments size: " << attachments.size()
            << std::endl;
  std::string rspMsg = "";
  size_t retry = 0;
  do {
    if (retry) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    fuzzing::Timer t;

    auto channel = CreateRpcClient();
    NYT::NApi::NRpcProxy::TApiServiceProxy proxy(channel);

    auto req = (proxy.*proxyMethod)();
    req->CopyFrom(request);
    {
      std::vector<NYT::NChunkClient::TBlock> blocks;
      for (const auto& attachment : attachments) {
        blocks.emplace_back(NYT::TSharedRef::FromString(TString(attachment)));
      }
      req->Attachments().reserve(blocks.size());
      for (const auto& block : blocks) {
        req->Attachments().push_back(block.Data);
      }
    }
    auto rspOrError = NYT::NConcurrency::WaitFor(req->Invoke());
    rspMsg = rspOrError.GetMessage();

    const auto after_rss = fuzzing::getCurrentRSS();
    std::cerr << "[FUZZER] " << methodName << " took " << t.Reset() << " ms, response: " << rspMsg
              << ", retry: " << retry << ", attachments size:" << attachments.size()
              << ", real attach size: " << req->Attachments().size()
              << ", start rss: " << before_rss << ", after rss: " << after_rss
              << ", diff: " << (after_rss - before_rss) * 1.0 / (1024 * 1024 * 1024) << "GB"
              << std::endl;
    ++retry;
  } while (rspMsg == "Channel terminated");
}

}  // anonymous namespace

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

  NYT::NApi::NRpcProxy::NProto::TRpcProxyFuzzerInput parsed_input;
  if (!parsed_input.ParseFromIstream(&str_input)) {
    std::cerr << "Failed to parse protobuf message." << std::endl;
    return -1;
  }
  google::protobuf::TextFormat::Printer printer;
  printer.SetUseUtf8StringEscaping(true);

  TProtoStringType text_message;
  std::cout << "Fuzzer input:\n";
  if (!printer.PrintToString(parsed_input, &text_message)) {
    std::cerr << "Failed to convert protobuf message to text." << std::endl;
    return -1;
  }

  std::cout << text_message << std::endl;
  Init();

  for (const auto& request_with_attachments : parsed_input.requests()) {
    const auto& request = request_with_attachments.request();
    const auto& attachments = request_with_attachments.attachments();
    switch (request.request_case()) {
      case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kCreateNode:
        SendRequest("CreateNode", request.create_node(),
                    &NYT::NApi::NRpcProxy::TApiServiceProxy::CreateNode, attachments);
        break;
      case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kCreateObject:
        SendRequest("CreateObject", request.create_object(),
                    &NYT::NApi::NRpcProxy::TApiServiceProxy::CreateObject, attachments);
        break;
      case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kListNode:
        SendRequest("ListNode", request.list_node(),
                    &NYT::NApi::NRpcProxy::TApiServiceProxy::ListNode, attachments);
        break;
      case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kGetNode:
        SendRequest("GetNode", request.get_node(), &NYT::NApi::NRpcProxy::TApiServiceProxy::GetNode,
                    attachments);
        break;
      case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kReadFile:
        SendRequest("ReadFile", request.read_file(),
                    &NYT::NApi::NRpcProxy::TApiServiceProxy::ReadFile, attachments);
        break;
      case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kWriteFile:
        SendRequest("WriteFile", request.write_file(),
                    &NYT::NApi::NRpcProxy::TApiServiceProxy::WriteFile, attachments);
        break;
      case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kReadTable:
        SendRequest("ReadTable", request.read_table(),
                    &NYT::NApi::NRpcProxy::TApiServiceProxy::ReadTable, attachments);
        break;
      case NYT::NApi::NRpcProxy::NProto::TFuzzerRpcProxySingleRequest::kWriteTable:
        SendRequest("WriteTable", request.write_table(),
                    &NYT::NApi::NRpcProxy::TApiServiceProxy::WriteTable, attachments);
        break;
      default:
        break;
    }
  }

  done = true;
  for (pid_t pid : childPIDs) {
    kill(pid, SIGTERM);
    int status;
    waitpid(pid, &status, 0);
  }

  std::cerr << "All child processes terminated. Exiting now." << std::endl;
  return 0;
}
