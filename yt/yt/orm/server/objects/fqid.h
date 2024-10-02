#pragma once

#include "public.h"

#include <yt/yt/orm/server/objects/proto/objects.pb.h>

#include <yt/yt/library/query/base/ast.h>

#include <yt/yt/orm/client/objects/key.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

// An FQID of an object is a fully qualified id in the form $service|$cluster|$type|$key|$uuid.
// - Service is a string identifying the ORM installation ("yp", "grut" etc.) It can also be changed
// if a new version is implemented (e.g., "yp2").
// - Cluster is an instance of the installation (for YP, "xdc", "vla", etc.)
// - Type is the schema id of the object.
// - Key is the object key serialized for API (composite fields concatenated with "+" ). The key is
// further CGI escaped (with %XX) to mask "|" (for cases of fqids being keys of other objects) and
// other special characters.
// - Uuid is the object uuid, randomly assigned on object creation.
// Example:
//   yp|sas|pod|abracadabra|abfada76-34dd-baed-c501-233e37a6ee2a
// Fqid is portable across clusters and changes when an object is deleted and recreated under
// the same id. For forward compatibility, always use library methods to process these.

// This fluent interface for reading and writing fqids implements canonical ways of looking up
// the components to reduce errors.
// Possible usage:
// TString fqid = TFqid(bootstrap)
//     .Schema("node").Key(node->GetKey()).Uuid(node->MetaEtc().Load())
//     .Serialize();

class TFqid
{
public:
    explicit TFqid(NMaster::IBootstrap* bootstrap);

    // Produces the canonical fqid form. Runs some sanity checks.
    TString Serialize() const;

    // Checks that all fields are well-formed, throws otherwise.
    void Validate() const;

    // Parses the canonical fqid form or throws. Does not validate.
    static TFqid Parse(NMaster::IBootstrap* bootstrap, TStringBuf serialized);

    // Getters
    const TString& DBName() const;
    TObjectTypeValue Type() const; // Note: Schema and Type are the same field.
    TObjectId Schema() const;
    const TObjectKey& Key() const;
    const TObjectId& Uuid() const;

    // Setters
    TFqid& DBName(TString cluster); // Usually already set from bootstrap in the ctor.
    TFqid& Type(TObjectTypeValue type);
    TFqid& Schema(TStringBuf schema);
    TFqid& Key(TObjectKey key);
    TFqid& Uuid(TObjectId uuid);
    TFqid& Uuid(const NProto::TMetaEtc& metaEtc);

private:
    NMaster::IBootstrap* Bootstrap_;
    TString DBName_;
    TObjectTypeValue Type_{TObjectTypeValues::Null};
    TObjectKey Key_;
    TObjectId Uuid_;
};

// TODO(ozzzernova): Format key in fqid with %xx.
NQueryClient::NAst::TExpressionPtr BuildFqidExpression(
    const IObjectTypeHandler* typeHandler,
    IQueryContext* context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
