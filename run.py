import uvicorn

import app

uvicorn.run(app.asgi_app, log_config=None, forwarded_allow_ips='*', host='0.0.0.0')
