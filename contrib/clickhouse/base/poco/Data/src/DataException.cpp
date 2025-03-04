//
// DataException.cpp
//
// Library: Data
// Package: DataCore
// Module:  DataException
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Data/DataException.h"
#include <typeinfo>


namespace DBPoco {
namespace Data {


DB_POCO_IMPLEMENT_EXCEPTION(DataException, DBPoco::IOException, "Database Exception")
DB_POCO_IMPLEMENT_EXCEPTION(RowDataMissingException, DataException, "Data for row missing")
DB_POCO_IMPLEMENT_EXCEPTION(UnknownDataBaseException, DataException, "Type of data base unknown")
DB_POCO_IMPLEMENT_EXCEPTION(UnknownTypeException, DataException, "Type of data unknown")
DB_POCO_IMPLEMENT_EXCEPTION(ExecutionException, DataException, "Execution error")
DB_POCO_IMPLEMENT_EXCEPTION(BindingException, DataException, "Binding error")
DB_POCO_IMPLEMENT_EXCEPTION(ExtractException, DataException, "Extraction error")
DB_POCO_IMPLEMENT_EXCEPTION(LimitException, DataException, "Limit error")
DB_POCO_IMPLEMENT_EXCEPTION(NotSupportedException, DataException, "Feature or property not supported")
DB_POCO_IMPLEMENT_EXCEPTION(SessionUnavailableException, DataException, "Session is unavailable")
DB_POCO_IMPLEMENT_EXCEPTION(SessionPoolExhaustedException, DataException, "No more sessions available from the session pool")
DB_POCO_IMPLEMENT_EXCEPTION(SessionPoolExistsException, DataException, "Session already exists in the pool")
DB_POCO_IMPLEMENT_EXCEPTION(NoDataException, DataException, "No data found")
DB_POCO_IMPLEMENT_EXCEPTION(LengthExceededException, DataException, "Data too long")
DB_POCO_IMPLEMENT_EXCEPTION(ConnectionFailedException, DataException, "Connection attempt failed")
DB_POCO_IMPLEMENT_EXCEPTION(NotConnectedException, DataException, "Not connected to data source")


} } // namespace DBPoco::Data
