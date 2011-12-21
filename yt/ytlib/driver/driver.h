#pragma once

#include "common.h"

#include "../misc/configurable.h"
#include "../misc/error.h"
#include "../ytree/ytree.h"
#include "../ytree/yson_events.h"
#include "../ytree/yson_writer.h"
// TODO: consider using forward declarations.
#include "../election/leader_lookup.h"
#include "../transaction_client/transaction_manager.h"
#include "../file_client/file_reader.h"
#include "../file_client/file_writer.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct IDriverStreamProvider
{
    virtual ~IDriverStreamProvider()
    { }

    virtual TAutoPtr<TInputStream>  CreateInputStream(const Stroka& spec) = 0;
    virtual TAutoPtr<TOutputStream> CreateOutputStream(const Stroka& spec) = 0;
    virtual TAutoPtr<TOutputStream> CreateErrorStream() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TDriver
    : private TNonCopyable
{
public:
    struct TConfig
        : TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        NYTree::TYsonWriter::EFormat OutputFormat;
        NElection::TLeaderLookup::TConfig::TPtr Masters;
        NTransactionClient::TTransactionManager::TConfig::TPtr TransactionManager;
        NFileClient::TFileReader::TConfig::TPtr FileDownloader;
        NFileClient::TFileWriter::TConfig::TPtr FileUploader;

        TConfig()
        {
            Register("output_format", OutputFormat).Default(NYTree::TYsonWriter::EFormat::Text);
            Register("masters", Masters);
            Register("transaction_manager", TransactionManager).DefaultNew();
            Register("file_downloader", FileDownloader).DefaultNew();
            Register("file_uploader", FileUploader).DefaultNew();
        }
    };

    TDriver(
        TConfig* config,
        IDriverStreamProvider* streamProvider);
    ~TDriver();

    TError Execute(const NYTree::TYson& request);

private:
    class TImpl;

    THolder<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

