import json
import os

from urllib.parse import quote

from flask import Flask, redirect, request, Response
from flask import Blueprint, render_template, url_for, jsonify

from jupyterhub.services.auth import HubAuth

from bokeh.embed import components
from bokeh.models import ColumnDataSource
from bokeh.models.widgets import DataTable, TableColumn
from bokeh.models.sources import AjaxDataSource

from openshift import oconfig
from openshift import oclient

from kubernetes import kclient

from wrapt import decorator

with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace') as fp:
    namespace = fp.read().strip()

oconfig.load_incluster_config()

corev1api = kclient.CoreV1Api()

auth = HubAuth(api_token=os.environ['JUPYTERHUB_API_TOKEN'],
        cookie_cache_max_age=60)

prefix = os.environ.get('JUPYTERHUB_SERVICE_PREFIX', '')

application = Flask(__name__)

controller = Blueprint('controller', __name__, template_folder='templates')

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

@controller.route('/')
@authenticated
def whoami(user):
    return Response(json.dumps(user, indent=1, sort_keys=True),
            mimetype='application/json')

dask_cluster_name = os.environ.get('DASK_CLUSTER_NAME')
dask_scheduler_name = '%s-scheduler' % dask_cluster_name
dask_worker_name = '%s-worker' % dask_cluster_name

@controller.route('/pods', methods=['GET', 'OPTIONS', 'POST'])
def tabledata():
    pods = corev1api.list_namespaced_pod(namespace)

    names = []

    for pod in pods.items:
        name = pod.metadata.labels.get('deploymentconfig')
        if name in [dask_scheduler_name, dask_worker_name]:
            names.append(pod.metadata.name)

    return jsonify(pods=sorted(names),)

@controller.route('/view')
def index():
    source = AjaxDataSource(data=dict(pods=[],),
            data_url=url_for('controller.pods'), polling_interval=1000)

    columns = [TableColumn(field="pods", title="Pod"),]

    data_table = DataTable(source=source, columns=columns, width=400, height=280)

    script, div = components(data_table)

    return render_template("view.html", script=script, div=div)

application.register_blueprint(controller, url_prefix=prefix.rstrip('/'))
