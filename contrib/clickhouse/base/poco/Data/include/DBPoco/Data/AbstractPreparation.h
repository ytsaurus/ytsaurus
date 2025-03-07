//
// AbstractPreparation.h
//
// Library: Data
// Package: DataCore
// Module:  AbstractPreparation
//
// Definition of the AbstractPreparation class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Data_AbstractPreparation_INCLUDED
#define DB_Data_AbstractPreparation_INCLUDED


#include <cstddef>
#include "DBPoco/Data/AbstractPreparator.h"
#include "DBPoco/Data/Data.h"
#include "DBPoco/SharedPtr.h"


namespace DBPoco
{
namespace Data
{


    class Data_API AbstractPreparation
    /// Interface for calling the appropriate AbstractPreparator method
    {
    public:
        typedef SharedPtr<AbstractPreparation> Ptr;
        typedef AbstractPreparator::Ptr PreparatorPtr;

        AbstractPreparation(PreparatorPtr pPreparator);
        /// Creates the AbstractPreparation.

        virtual ~AbstractPreparation();
        /// Destroys the AbstractPreparation.

        virtual void prepare() = 0;
        /// Prepares data.

    protected:
        AbstractPreparation();
        AbstractPreparation(const AbstractPreparation &);
        AbstractPreparation & operator=(const AbstractPreparation &);

        PreparatorPtr preparation();
        /// Returns the preparation object

        PreparatorPtr _pPreparator;
    };


    //
    // inlines
    //
    inline AbstractPreparation::PreparatorPtr AbstractPreparation::preparation()
    {
        return _pPreparator;
    }


}
} // namespace DBPoco::Data


#endif // DB_Data_AbstractPreparation_INCLUDED
