#include "utils.h"
#include "syncword.h"

#include <google/protobuf/io/coded_stream.h>

namespace NFraming::NPrivate {
    size_t GetProtoseqFrameSize(size_t dataSize) {
        return dataSize + PROTOSEQ_FRAME_LEN_BYTES + SyncWord.size();
    }

    size_t GetLenvalFrameSize(size_t dataSize) {
        return google::protobuf::io::CodedOutputStream::VarintSize64(dataSize) + dataSize;
    }

    size_t GetLightProtoseqFrameSize(size_t dataSize) {
        return dataSize + PROTOSEQ_FRAME_LEN_BYTES;
    }
}
