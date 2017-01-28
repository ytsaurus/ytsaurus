#pragma once

#include <util/generic/fwd.h>
#include <util/system/types.h>

namespace google {
    namespace protobuf {
        class Message;
    }
}

namespace NYT {

    ////////////////////////////////////////////////////////////////////////////////
    // client.h
    ////////////////////////////////////////////////////////////////////////////////

    enum ELockMode : int;

    struct TStartTransactionOptions;

    struct TLockOptions;

    template <class TDerived>
    struct TTabletOptions;

    struct TMountTableOptions;

    struct TUnmountTableOptions;

    struct TRemountTableOptions;

    struct TReshardTableOptions;

    struct TAlterTableOptions;

    struct TLookupRowsOptions;

    struct TSelectRowsOptions;

    struct TCreateClientOptions;

    class ITransaction;
    using ITransactionPtr = TIntrusivePtr<ITransaction>;

    class IClientBase;

    class IClient;

    using IClientPtr = TIntrusivePtr<IClient>;
    using IClientBasePtr = TIntrusivePtr<IClientBase>;

    ////////////////////////////////////////////////////////////////////////////////
    // cypress.h
    ////////////////////////////////////////////////////////////////////////////////

    enum ENodeType : int;

    struct TCreateOptions;

    struct TRemoveOptions;

    struct TGetOptions;

    struct TListOptions;

    struct TCopyOptions;

    struct TMoveOptions;

    struct TLinkOptions;

    struct TConcatenateOptions;

    class ICypressClient;

    ////////////////////////////////////////////////////////////////////////////////
    // errors.h
    ////////////////////////////////////////////////////////////////////////////////

    class TOperationFailedError;

    ////////////////////////////////////////////////////////////////////////////////
    // node.h
    ////////////////////////////////////////////////////////////////////////////////

    class TNode;

    ////////////////////////////////////////////////////////////////////////////////
    // common.h
    ////////////////////////////////////////////////////////////////////////////////

    using TTransactionId = TGUID;
    using TNodeId = TGUID;
    using TLockId = TGUID;
    using TOperationId = TGUID;
    using TTabletCellId = TGUID;

    using TYPath = Stroka;
    using TLocalFilePath = Stroka;

    template <class T>
    struct TKeyBase;

    // key column values
    using TKey = TKeyBase<TNode>;

    // key column names
    using TKeyColumns = TKeyBase<Stroka>;

    enum EValueType : int;

    enum ESortOrder : int;

    struct TColumnSchema;

    struct TTableSchema;

    struct TReadLimit;

    struct TReadRange;

    struct TRichYPath;

    struct TAttributeFilter;

    ////////////////////////////////////////////////////////////////////////////////
    // io.h
    ////////////////////////////////////////////////////////////////////////////////

    class IFileReader;

    using IFileReaderPtr = TIntrusivePtr<IFileReader>;

    class IFileWriter;

    using IFileWriterPtr = TIntrusivePtr<IFileWriter>;

    template <class T, class = void>
    class TTableReader;

    template <class T>
    using TTableReaderPtr = TIntrusivePtr<TTableReader<T>>;

    template <class T, class = void>
    class TTableWriter;

    template <class T>
    using TTableWriterPtr = TIntrusivePtr<TTableWriter<T>>;

    struct TYaMRRow;

    using ::google::protobuf::Message;

    using TYaMRReader = TTableReader<TYaMRRow>;
    using TYaMRWriter = TTableWriter<TYaMRRow>;
    using TNodeReader = TTableReader<TNode>;
    using TNodeWriter = TTableWriter<TNode>;
    using TMessageReader = TTableReader<Message>;
    using TMessageWriter = TTableWriter<Message>;

    template <class TDerived>
    struct TIOOptions;

    struct TFileReaderOptions;

    struct TFileWriterOptions;

    struct TTableReaderOptions;

    struct TTableWriterOptions;
}
