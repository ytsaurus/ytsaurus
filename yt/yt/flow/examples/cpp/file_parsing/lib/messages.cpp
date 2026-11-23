#include "messages.h"

namespace NYT::NFlow::NFileParsingExample {

////////////////////////////////////////////////////////////////////////////////

void TParseTask::Register(TRegistrar registrar)
{
    registrar.Parameter("url", &TThis::Url)
        .Default();
    registrar.Parameter("size", &TThis::Size)
        .Default();
    registrar.Parameter("attempt", &TThis::Attempt)
        .Default();
    registrar.Parameter("first_seen_at", &TThis::FirstSeenAt)
        .Default();
}

void TParseResult::Register(TRegistrar registrar)
{
    registrar.Parameter("rejected", &TThis::Rejected)
        .Default();
    registrar.Parameter("row_count", &TThis::RowCount)
        .Default();
    registrar.Parameter("worker", &TThis::Worker)
        .Default();
}

void TParsedFile::Register(TRegistrar registrar)
{
    registrar.Parameter("url", &TThis::Url)
        .Default();
    registrar.Parameter("row_count", &TThis::RowCount)
        .Default();
    registrar.Parameter("attempt", &TThis::Attempt)
        .Default();
    registrar.Parameter("worker", &TThis::Worker)
        .Default();
}

void TFailedFile::Register(TRegistrar registrar)
{
    registrar.Parameter("url", &TThis::Url)
        .Default();
    registrar.Parameter("attempt", &TThis::Attempt)
        .Default();
    registrar.Parameter("error", &TThis::Error)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileParsingExample
