#pragma once

#include "public.h"

#include <ytlib/election/leader_lookup.h>
#include <ytlib/table_client/yson_table_input.h>
#include <ytlib/scheduler/job.pb.h>

class TOutputStream;

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct IUserJobIO {
    virtual ~IUserJobIO();

    virtual int GetInputCount() const = 0;
    virtual int GetOutputCount() const = 0;

    /*!
     *  \param output - stream, where returned table input writes table data.
     */
    virtual TAutoPtr<NTableClient::TYsonTableInput> CreateTableInput(
        int index, 
        TOutputStream* output) const = 0;

    virtual TAutoPtr<TOutputStream> CreateTableOutput(int index) const = 0;
    virtual TAutoPtr<TOutputStream> CreateErrorOutput() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

TAutoPtr<IUserJobIO> CreateUserJobIO(
    const TJobIOConfigPtr ioConfig,
    const NElection::TLeaderLookup::TConfigPtr mastersConfig,
    const NScheduler::NProto::TJobSpec& jobSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT