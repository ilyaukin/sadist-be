import io

import requests
from PIL import Image
from app import logger
from bson import ObjectId
from db import write_grid_file, read_grid_file
from flask import session
from user_helper import anon_

IMAGE_CONFIG = {
    'avatar': {
        'max_size': 20 * 1024,  # 20Kb
        'max_dimensions': (100, 100),
        # write permissions
        'w': ['username', 'anon'],
        'path_prefix': 'img/avatar/'
    }
}

def check_image_props(data, image_type):
    """
    Check image props… such as size and dimensions
    """
    config = IMAGE_CONFIG.get(image_type)
    if not config:
        raise ValueError(f"Unknown image type: {image_type}")

    if len(data) > config.get('max_size', float('inf')):
        raise ValueError(f"Image size exceeds limit: {len(data)} > {config['max_size']}")

    if 'max_dimensions' in config:
        try:
            with Image.open(io.BytesIO(data)) as img:
                width, height = img.size
                max_w, max_h = config['max_dimensions']
                if width > max_w or height > max_h:
                    raise ValueError(f"Image dimensions exceed limit: {width}x{height} > {max_w}x{max_h}")
        except Exception as e:
            if isinstance(e, ValueError):
                raise e
            raise ValueError(f"Invalid image data: {str(e)}")

def check_image_permissions(data, image_type, permission):
    config = IMAGE_CONFIG.get(image_type)
    if not config:
        return False

    allowed_roles = config.get(permission, [])
    if 'anon' in allowed_roles:
        return True

    user_data = session.get('user', anon_)
    if user_data and user_data.get('type') != 'anon':
        return 'username' in allowed_roles

    return False

def create_image(data, filename, image_type, content_type=None):
    config = IMAGE_CONFIG.get(image_type)
    if not config:
        raise ValueError(f"Unknown image type: {image_type}")

    check_image_permissions(data, image_type, 'w')
    check_image_props(data, image_type)

    # random postfix to be able to save different files for the same
    # name passed by user
    filename += '_' + str(ObjectId())

    config = IMAGE_CONFIG.get(image_type)
    full_filename = f"{config['path_prefix']}{filename}"
    write_grid_file(data, full_filename, content_type)
    return filename

def create_image_from_url(url, filename, image_type):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.content
        content_type = response.headers.get('Content-Type')
        return create_image(data, filename, image_type, content_type)
    except Exception as e:
        logger.error(f"Failed to download image from {url}: {str(e)}")
        return None

def get_image(image_type, filename):
    config = IMAGE_CONFIG.get(image_type)
    if not config:
        return None
    full_filename = f"{config['path_prefix']}{filename}"
    return read_grid_file(full_filename)
