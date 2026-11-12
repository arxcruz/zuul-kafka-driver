"""Write zuul-manifest.json so the Zuul web UI can browse the logs."""

import json
import mimetypes
import os
import sys


def walk(path):
    entries = []
    for name in sorted(os.listdir(path)):
        full = os.path.join(path, name)
        st = os.stat(full)
        if os.path.isdir(full):
            entries.append({
                'name': name,
                'mimetype': 'application/directory',
                'encoding': None,
                'last_modified': int(st.st_mtime),
                'children': walk(full),
            })
        else:
            mimetype, encoding = mimetypes.guess_type(name)
            entries.append({
                'name': name,
                'mimetype': mimetype or 'text/plain',
                'encoding': encoding,
                'last_modified': int(st.st_mtime),
                'size': st.st_size,
            })
    return entries


root = sys.argv[1]
manifest = {'tree': walk(root), 'index_links': False}
with open(os.path.join(root, 'zuul-manifest.json'), 'w') as f:
    json.dump(manifest, f)
