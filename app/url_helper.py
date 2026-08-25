import os

from flask import has_request_context, request


def get_base_url(default=None):
    base_url = os.environ.get('BASE_URL') or (request.host_url.rstrip('/') if has_request_context() else None)
    if not base_url:
        if default is not None:
            return default
        raise Exception('BASE_URL is not set')
    return base_url