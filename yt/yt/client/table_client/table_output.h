#pragma once

#include "public.h"

#include <yt/client/formats/parser.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TTableOutput
    : public IOutputStream
{
public:
    explicit TTableOutput(std::unique_ptr<NFormats::IParser> parser);
    ~TTableOutput() override;

private:
    void DoWrite(const void* buf, size_t len);
    void DoFinish();

    const std::unique_ptr<NFormats::IParser> Parser_;

    bool ParserValid_ = true;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
