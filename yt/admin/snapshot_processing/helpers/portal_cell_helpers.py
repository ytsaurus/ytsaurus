portal_cell_role = "cypress_node_host"


def get_portal_cell_ids(yt_client):
    portal_cell_ids = []
    cell_descriptors = yt_client.get("//sys/@config/multicell_manager/cell_descriptors")

    try:
        items = cell_descriptors.iteritems()
    except AttributeError:
        items = cell_descriptors.items()
    for cell_id, descriptor in items:
        if portal_cell_role in descriptor.get("roles", []):
            portal_cell_ids.append(cell_id)

    return portal_cell_ids
