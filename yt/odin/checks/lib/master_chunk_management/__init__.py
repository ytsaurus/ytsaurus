UNKNOWN = 0
NOT_EXISTS = 1
EXISTS_AND_FALSE = 2
EXISTS_AND_TRUE = 3


def get_results(status_flag, states):
    results_dispatcher = {
        UNKNOWN: (states.UNAVAILABLE_STATE, "Unknown response for get {}."),
        NOT_EXISTS: (states.FULLY_AVAILABLE_STATE, "Can't find {}."),
        EXISTS_AND_TRUE: (states.FULLY_AVAILABLE_STATE, "{} is true."),
        EXISTS_AND_FALSE: (states.UNAVAILABLE_STATE, "{} is false!"),
    }
    return results_dispatcher.get(status_flag, results_dispatcher.get(UNKNOWN))


def get_bool_attribute_if_exists(yt_client, attribute_path):
    if not yt_client.exists(attribute_path):
        return NOT_EXISTS
    if yt_client.get(attribute_path):
        return EXISTS_AND_TRUE
    return EXISTS_AND_FALSE
