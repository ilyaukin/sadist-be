import io

from app import app, logger
from app.image import create_image, get_image
from flask import request, send_file


@app.route('/image/<image_type>', methods=['POST'])
def create_image_route(image_type):
    file = request.files.get('file') or request.files.get(image_type)
    if not file:
        return {'success': False, 'error': 'No file part'}, 400

    filename = file.filename or image_type

    try:
        data = file.read()
        saved_filename = create_image(data, filename, image_type, content_type=file.content_type)

        return {
            'success': True,
            'path': f"/image/{image_type}/{saved_filename}",
        }
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return {'success': False, 'error': str(e)}, 400

@app.route('/image/<image_type>/<filename>', methods=['GET'])
def get_image_route(image_type, filename):
    file = get_image(image_type, filename)
    if not file:
        return "Not found", 404
    
    return send_file(
        io.BytesIO(file.read()),
        mimetype=file.content_type or 'image/jpeg'
    )
