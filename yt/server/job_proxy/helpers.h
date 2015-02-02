#pragma once

#include "public.h"

#include <ytlib/formats/format.h>

#include <ytlib/table_client/public.h>

#include <core/misc/blob_output.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TContextPreservingInput
    : public TRefCounted
{
public:
    TContextPreservingInput(
        NTableClient::ISyncReaderPtr reader, 
        const NFormats::TFormat& format, 
        bool enableTableSwitch);

    void PipeReaderToOutput(TOutputStream* outputStream);

    TBlob GetContext() const;

private:
    NTableClient::ISyncReaderPtr Reader_;

    NFormats::TFormat Format_;

    bool EnableTableSwitch_;

    TBlobOutput CurrentBuffer_;
    TBlobOutput PreviousBuffer_;

};

DEFINE_REFCOUNTED_TYPE(TContextPreservingInput);

////////////////////////////////////////////////////////////////////////////////

void PipeInputToOutput(TInputStream* input, TOutputStream* output);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
} // namespace NJobProxy
