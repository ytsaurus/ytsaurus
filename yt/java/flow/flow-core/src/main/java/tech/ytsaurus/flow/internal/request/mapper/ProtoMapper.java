package tech.ytsaurus.flow.internal.request.mapper;

/**
 * Bidirectional mapper between a protobuf type and a Java domain type.
 * <p>
 * Implementations of this interface encapsulate all knowledge about how to convert
 * a specific domain type to/from its protobuf representation.
 * </p>
 *
 * @param <P> Protobuf type
 * @param <D> Domain type
 */
public interface ProtoMapper<P, D> {

    /**
     * Converts a protobuf object to a domain object.
     *
     * @param proto the protobuf object
     * @return the domain object
     */
    D fromProto(P proto);

    /**
     * Converts a domain object to a protobuf object.
     *
     * @param domain the domain object
     * @return the protobuf object
     */
    P toProto(D domain);
}
