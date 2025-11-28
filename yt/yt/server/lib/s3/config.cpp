#include "config.h"

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////////

void TS3ReaderConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("validate_block_checksums", &TThis::ValidateBlockChecksums)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TS3WriterConfig::Register(TRegistrar registrar) {
    registrar.Parameter("upload_part_size", &TThis::UploadPartSize)
        .GreaterThanOrEqual(MinMultiPartUploadPartSize)
        .Default(64_MB);
    registrar.Parameter("upload_window_size", &TThis::UploadWindowSize)
        .GreaterThan(0)
        .Default(128_MB);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
