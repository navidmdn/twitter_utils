from data_collection.api_wrapper.v1_wrapper import V1ApiWrapper
from data_collection.api_wrapper.api_wrapper import ApiWrapper


def create_api_wrapper(version) -> ApiWrapper:
    api = None
    if version == 2:
        raise NotImplementedError()
    elif version == 1:
        api = V1ApiWrapper()

    api.authenticate()
    return api
