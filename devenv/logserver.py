"""Serve /srv/static/logs with CORS so the Zuul web UI can read the logs."""

import functools
import http.server


class Handler(http.server.SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        super().end_headers()


handler = functools.partial(Handler, directory='/srv/static/logs')
http.server.ThreadingHTTPServer(('0.0.0.0', 8000), handler).serve_forever()
