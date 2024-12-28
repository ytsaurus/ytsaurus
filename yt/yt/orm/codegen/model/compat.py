# COMPAT will be removed after migrating to ORM codegen.
_YP = False


def set_yp():
    global _YP

    _YP = True


def yp():
    return _YP
