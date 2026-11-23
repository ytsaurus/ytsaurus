#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/yson_message.h>

namespace NYT::NFlow::NFileParsingExample {

////////////////////////////////////////////////////////////////////////////////

//! A request to parse a file: a row of the parse queue and a task of the async processor of #TParseFunction.
struct TParseTask
    : public TYsonMessage
{
    std::string Url;
    //! The size of the file; the stub parse takes time proportional to it.
    i64 Size = 0;
    //! The number of the earlier attempts, all rejected. The key of #TParseFunction includes it,
    //! so the next attempt has another key and may land in another partition and on another worker;
    //! it can also land in the same one.
    i64 Attempt = 0;
    //! The write time of the input row in the input queue, in milliseconds since the epoch;
    //! the same for all attempts. A busy executor serves the oldest files first.
    ui64 FirstSeenAt = 0;

    REGISTER_YSON_STRUCT(TParseTask);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TParseTask);

////////////////////////////////////////////////////////////////////////////////

//! The result of a parse.
struct TParseResult
    : public TYsonMessage
{
    //! The executor found no free slot for the task in time; the other fields are unset.
    bool Rejected = false;
    i64 RowCount = 0;
    //! The executor instance that parsed the file.
    std::string Worker;

    REGISTER_YSON_STRUCT(TParseResult);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TParseResult);

////////////////////////////////////////////////////////////////////////////////

//! A row of the output of the parsed files.
struct TParsedFile
    : public TYsonMessage
{
    std::string Url;
    i64 RowCount = 0;
    //! The attempt that parsed the file.
    i64 Attempt = 0;
    std::string Worker;

    REGISTER_YSON_STRUCT(TParsedFile);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TParsedFile);

////////////////////////////////////////////////////////////////////////////////

//! A row of the output of the files that could not be parsed.
struct TFailedFile
    : public TYsonMessage
{
    std::string Url;
    //! The last attempt.
    i64 Attempt = 0;
    std::string Error;

    REGISTER_YSON_STRUCT(TFailedFile);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFailedFile);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileParsingExample
