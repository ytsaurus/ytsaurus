#include <google/protobuf/text_format.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>
#include <yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.pb.h>

#include <fstream>
#include <iostream>
#include <string>

template <typename T>
bool tryParseAndPrint(std::fstream& input) {
  T message;
  if (!message.ParseFromIstream(&input)) {
    return false;
  }

  google::protobuf::TextFormat::Printer printer;
  printer.SetUseUtf8StringEscaping(true);
  TString text_message;
  if (!printer.PrintToString(message, &text_message)) {
    std::cerr << "Failed to convert protobuf message to text." << std::endl;
    return false;
  }

  std::cout << text_message << std::endl;
  return true;
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

  if (!tryParseAndPrint<NYT::NChunkClient::NProto::TDatanodeFuzzerInput>(str_input)) {
    str_input.clear();
    str_input.seekg(0, std::ios::beg);

    if (!tryParseAndPrint<NYT::NApi::NRpcProxy::NProto::TRpcProxyFuzzerInput>(str_input)) {
      std::cerr << "Failed to parse protobuf message with both expected types." << std::endl;
      return -1;
    }
  }

  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
