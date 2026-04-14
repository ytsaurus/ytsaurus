#include "helpers.h"

#include <yt/yt/server/lib/transaction_server/private.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NTransactionServer {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TransactionServerLogger;

////////////////////////////////////////////////////////////////////////////////

ETransactionType ObjectTypeToTransactionType(NObjectClient::EObjectType objectType)
{
    auto transactionType = ETransactionType::Unknown;

    switch (objectType) {
        case EObjectType::Transaction:
        case EObjectType::NestedTransaction:
            transactionType = ETransactionType::Cypress;
            break;

        case EObjectType::ExternalizedTransaction:
        case EObjectType::ExternalizedNestedTransaction:
            transactionType = ETransactionType::Externalized;
            break;

        case EObjectType::UploadTransaction:
        case EObjectType::UploadNestedTransaction:
            transactionType = ETransactionType::Upload;
            break;

        case EObjectType::SystemTransaction:
        case EObjectType::SystemNestedTransaction:
            transactionType = ETransactionType::System;
            break;

        case EObjectType::AtomicTabletTransaction:
            transactionType = ETransactionType::Sequoia;
            break;

        default:
            break;
    }

    return transactionType;
}

EObjectType TransactionTypeToObjectType(ETransactionType transactionType, bool nested, TTransactionId transactionId)
{
    auto transactionObjectType = EObjectType::Null;
    switch (transactionType) {
        case ETransactionType::Cypress:
            transactionObjectType = nested
                ? EObjectType::NestedTransaction
                : EObjectType::Transaction;
            break;

        case ETransactionType::Externalized:
            transactionObjectType = nested
                ? EObjectType::ExternalizedNestedTransaction
                : EObjectType::ExternalizedTransaction;
            break;

        case ETransactionType::Upload:
            transactionObjectType = nested
                ? EObjectType::UploadNestedTransaction
                : EObjectType::UploadTransaction;
            break;

        case ETransactionType::System:
            transactionObjectType = nested
                ? EObjectType::SystemNestedTransaction
                : EObjectType::SystemTransaction;
            break;

        case ETransactionType::Sequoia: {
            YT_LOG_FATAL_IF(
                nested,
                "Sequoia transactions cannot be nested (TransactionId: %v)",
                transactionId);

            transactionObjectType = EObjectType::AtomicTabletTransaction;
            break;
        }

        case ETransactionType::Unknown:
            YT_LOG_ALERT("Attempting to deduce object type of an unknown transaction type (HintId: %v)",
                transactionId);
            break;
    }

    return transactionObjectType;
}

bool ValidateTransactionTypeCoherency(ETransactionType transactionType, TTransactionId transactionId)
{
    auto typeFromId = TypeFromId(transactionId);
    bool ok = false;
    if (transactionType == ETransactionType::Cypress && IsCypressTransactionType(typeFromId)) {
        ok = true;
    } else if (transactionType == ETransactionType::Externalized && IsExternalizedTransactionType(typeFromId)) {
        ok = true;
    } else if (transactionType == ETransactionType::Upload && IsUploadTransactionType(typeFromId)) {
        ok = true;
    } else if (transactionType == ETransactionType::System &&
        (typeFromId == EObjectType::SystemTransaction || typeFromId == EObjectType::SystemNestedTransaction))
    {
        ok = true;
    } else if (transactionType == ETransactionType::Sequoia && typeFromId == EObjectType::AtomicTabletTransaction) {
        ok = true;
    }

    YT_LOG_ALERT_UNLESS(
        ok,
        "Transaction type differs from a transaction type deduced from its ID "
        "(ExpectedType: %v, DeducedType: %v, TransactionId: %v, ObjectType: %v)",
        transactionType,
        ObjectTypeToTransactionType(typeFromId),
        transactionId,
        typeFromId);

    return ok;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
