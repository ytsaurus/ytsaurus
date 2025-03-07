//
// DataException.h
//
// Library: Data
// Package: DataCore
// Module:  DataException
//
// Definition of the DataException class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Data_DataException_INCLUDED
#define DB_Data_DataException_INCLUDED


#include "DBPoco/Data/Data.h"
#include "DBPoco/Exception.h"


namespace DBPoco
{
namespace Data
{


    DB_POCO_DECLARE_EXCEPTION(Data_API, DataException, DBPoco::IOException)
    DB_POCO_DECLARE_EXCEPTION(Data_API, RowDataMissingException, DataException)
    DB_POCO_DECLARE_EXCEPTION(Data_API, UnknownDataBaseException, DataException)
    DB_POCO_DECLARE_EXCEPTION(Data_API, UnknownTypeException, DataException)
    DB_POCO_DECLARE_EXCEPTION(Data_API, ExecutionException, DataException)
    DB_POCO_DECLARE_EXCEPTION(Data_API, BindingException, DataException)
    DB_POCO_DECLARE_EXCEPTION(Data_API, ExtractException, DataException)
    DB_POCO_DECLARE_EXCEPTION(Data_API, LimitException, DataException)
    DB_POCO_DECLARE_EXCEPTION(Data_API, NotSupportedException, DataException)
    DB_POCO_DECLARE_EXCEPTION(Data_API, SessionUnavailableException, DataException)
    DB_POCO_DECLARE_EXCEPTION(Data_API, SessionPoolExhaustedException, DataException)
    DB_POCO_DECLARE_EXCEPTION(Data_API, SessionPoolExistsException, DataException)
    DB_POCO_DECLARE_EXCEPTION(Data_API, NoDataException, DataException)
    DB_POCO_DECLARE_EXCEPTION(Data_API, LengthExceededException, DataException)
    DB_POCO_DECLARE_EXCEPTION(Data_API, ConnectionFailedException, DataException)
    DB_POCO_DECLARE_EXCEPTION(Data_API, NotConnectedException, DataException)


}
} // namespace DBPoco::Data


#endif // DB_Data_DataException_INCLUDED
