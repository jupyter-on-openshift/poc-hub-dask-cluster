import json
import os

from urllib.parse import quote

from flask import Flask, redirect, request, Response
from flask import render_template

from jupyterhub.services.auth import HubAuth
from wrapt import decorator

prefix = os.environ.get('JUPYTERHUB_SERVICE_PREFIX', '/')

auth = HubAuth(api_token=os.environ['JUPYTERHUB_API_TOKEN'],
        cookie_cache_max_age=60)

application = Flask(__name__)

@decorator
def authenticated(wrapped, instance, args, kwargs):
    cookie = request.cookies.get(auth.cookie_name)
    token = request.headers.get(auth.auth_header_name)

    if cookie:
        user = auth.user_for_cookie(cookie)
    elif token:
        user = auth.user_for_token(token)
    else:
        user = None

    if user:
        return wrapped(user, *args, **kwargs)
    else:
        # Request to login url on failed authentication.
        return redirect(auth.login_url + '?next=%s' % quote(request.path))

@application.route(prefix)
@authenticated
def whoami(user):
    return Response(json.dumps(user, indent=1, sort_keys=True),
            mimetype='application/json')
