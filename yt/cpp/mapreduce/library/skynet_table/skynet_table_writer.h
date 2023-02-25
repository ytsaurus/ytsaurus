#pragma once

#include <yt/cpp/mapreduce/library/skynet_table/skynet_table_row.pb.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <util/generic/ptr.h>

namespace NYtSkynetTable {

////////////////////////////////////////////////////////////////////////////////

//
// Allows to write skynet tables.
//
//   https://wiki.yandex-team.ru/yt/userdoc/blobtables/#skynet
class ISkynetTableWriter
    : public TThrRefBase
{
public:
    // If Finish was never called it will be called in the destructor.
    // Though it's recommended to call Finish explicitly.
    // If error happens during explicit call exception is thrown.
    // If error happens during implicit call in the destructor (except when stack is unwinding due to other exception)
    // program will terminate with abort to avoid silent data loss.
    virtual ~ISkynetTableWriter();

    //
    // Start writing new file into the table.
    //
    // NB. Usually skynet tables are sorted by filename so users have to add files respecting this order
    // i.e file 'aaa.txt' must go before file 'zzz.txt'.
    virtual NYT::IFileWriterPtr AppendFile(TString fileName) = 0;

    //
    // Finish uploading single file.
    virtual void Finish() = 0;
};

using ISkynetTableWriterPtr = ::TIntrusivePtr<ISkynetTableWriter>;

//
// Creates skynet table with proper schema and attributes.
// Internally it calls IClientBase::Create and similar behaviour,
// e.g. by default it will throw error if table already exists.
NYT::TNodeId CreateSkynetTable(
    NYT::IClientBasePtr client,
    const NYT::TYPath& path,
    const NYT::TCreateOptions& options = NYT::TCreateOptions());

//
// Create writer to write table from the client.
// If table doesn't exist it will be created. Otherwise existing table will used.
// All content of a table will be overwritten.
ISkynetTableWriterPtr CreateSkynetWriter(const NYT::IClientBasePtr& client, const NYT::TYPath& path);

//
// Create writers to write skynet table from the job.
// Its compatible only with protobuf jobs and user must use AddOutput<TSkynetTableRow>(...) in operation spec.
// NB. Use CreateSkynetTable to create table before operation start.
ISkynetTableWriterPtr CreateSkynetWriter(NYT::TTableWriter<TSkynetTableRow>* writer, size_t tableIndex = 0);
ISkynetTableWriterPtr CreateSkynetWriter(NYT::TTableWriter<::google::protobuf::Message>* writer, size_t tableIndex = 0);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtSkynetTable
