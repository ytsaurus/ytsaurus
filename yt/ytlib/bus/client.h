#pragma once

#include "common.h"
#include "bus.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

//! A client IBus factory.
/*!
 *  Thread affinity: any.
 */
struct IBusClient
    : public virtual TRefCounted
{
public:
    typedef TIntrusivePtr<IBusClient> TPtr;

    //! Creates a new bus.
    /*!
     *  The bus will point to the address supplied during construction.
     *
     *  \param handler A handler that will process incoming messages.
     *  \return A new bus.
     *
     */
    virtual IBus::TPtr CreateBus(IMessageHandler::TPtr handler) = 0;
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
