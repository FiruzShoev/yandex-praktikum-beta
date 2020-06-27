from flask import abort
from api_errors import BAD_REQUEST_BODY


def validate_param_is_positive_int(param):
    try:
        param = int(param)
    except ValueError:
        abort(*BAD_REQUEST_BODY)

    if param <= 0:
        abort(*BAD_REQUEST_BODY)

    return param


def validate_param_is_allowed(param, allowed_values):
    if param not in allowed_values:
        abort(*BAD_REQUEST_BODY)

    return param
