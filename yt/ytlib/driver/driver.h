#pragma once

#include "common.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/misc/error.h>
#include <ytlib/ytree/ytree.h>
#include <ytlib/ytree/yson_consumer.h>
#include <ytlib/ytree/yson_writer.h>
// TODO: consider using forward declarations.
#include <ytlib/election/leader_lookup.h>
#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/file_client/file_reader.h>
#include <ytlib/file_client/file_writer.h>
#include <ytlib/table_client/table_reader.h>
#include <ytlib/table_client/table_writer.h>

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
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        NYTree::EFormat OutputFormat;
        NElection::TLeaderLookup::TConfig::TPtr Masters;
        NTransactionClient::TTransactionManager::TConfig::TPtr TransactionManager;
        NFileClient::TFileReader::TConfig::TPtr FileReader;
        NFileClient::TFileWriter::TConfig::TPtr FileWriter;
        NTableClient::TTableReader::TConfig::TPtr TableReader;
        NTableClient::TTableWriter::TConfig::TPtr TableWriter;

        TConfig()
        {
            Register("output_format", OutputFormat).Default(NYTree::EFormat::Text);
            Register("masters", Masters);
            Register("transaction_manager", TransactionManager).DefaultNew();
            Register("file_reader", FileReader).DefaultNew();
            Register("file_writer", FileWriter).DefaultNew();
            Register("table_reader", TableReader).DefaultNew();
            Register("table_writer", TableWriter).DefaultNew();
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

