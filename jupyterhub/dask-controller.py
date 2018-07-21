import os
import time
import json

from urllib.parse import quote

from flask import Flask, redirect, request, Response, abort
from flask import Blueprint, jsonify

from jupyterhub.services.auth import HubAuth

os.environ['KUBERNETES_SERVICE_HOST'] = 'openshift.default.svc.cluster.local'
os.environ['KUBERNETES_SERVICE_PORT'] = '443'

from openshift import config as oconfig
from openshift import client as oclient

from kubernetes import client as kclient
from kubernetes.client.models import V1ObjectMeta, V1Scale, V1ScaleSpec

from wrapt import decorator

with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace') as fp:
    namespace = fp.read().strip()

oconfig.load_incluster_config()

corev1api = kclient.CoreV1Api()
appsopenshiftiov1api = oclient.AppsOpenshiftIoV1Api()

auth = HubAuth(api_token=os.environ['JUPYTERHUB_API_TOKEN'],
        cookie_cache_max_age=60)

jupyterhub_service_name = os.environ.get('JUPYTERHUB_SERVICE_NAME', '')
prefix = os.environ.get('JUPYTERHUB_SERVICE_PREFIX', '')

application = Flask(__name__)

controller = Blueprint('controller', __name__, template_folder='templates')

@decorator
def authenticated_user(wrapped, instance, args, kwargs):
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

@decorator
def admin_users_only(wrapped, instance, args, kwargs):
    user = (lambda user: user)(*args, **kwargs)
    if not user.get('admin', False):
        abort(403)
    return wrapped(*args, **kwargs)

dask_cluster_name = os.environ.get('DASK_CLUSTER_NAME')
dask_scheduler_name = '%s-scheduler' % dask_cluster_name
dask_worker_name = '%s-worker' % dask_cluster_name

def get_pods():
    pods = corev1api.list_namespaced_pod(namespace)

    details = []

    for pod in pods.items:
        name = pod.metadata.labels.get('deploymentconfig')
        if name == dask_worker_name:
            details.append((pod.metadata.name, pod.status.phase))

    return details

@controller.route('/pods', methods=['GET', 'OPTIONS', 'POST'])
@authenticated_user
def pods(user):
    return jsonify(get_pods())

@controller.route('/scale', methods=['GET', 'OPTIONS', 'POST'])
@authenticated_user
@admin_users_only
def scale(user):
    replicas = request.args.get('replicas', None)

    if replicas is None:
        return jsonify()

    replicas = int(replicas)

    scale = V1Scale()
    scale.kind = 'Scale'
    scale.api_version = 'extensions/v1beta1'

    name = '%s-worker' % dask_cluster_name

    scale.metadata = V1ObjectMeta(
            namespace=namespace, name=name,
            labels={'app': dask_cluster_name})

    scale.spec = V1ScaleSpec(replicas=replicas)

    result = appsopenshiftiov1api.replace_namespaced_deployment_config_scale(
            name, namespace, scale)

    return jsonify()

@controller.route('/restart', methods=['GET', 'OPTIONS', 'POST'])
@authenticated_user
@admin_users_only
def restart(user):
    patch = {
        'spec': {
            'template': {
                'metadata': {
                    'annotations': {
                        'dask-controller/restart': str(time.time())
                    }
                }
            }
        }
    }

    name = '%s-worker' % dask_cluster_name

    appsopenshiftiov1api.patch_namespaced_deployment_config(
            name, namespace, patch)

    return jsonify()

application.register_blueprint(controller, url_prefix=prefix.rstrip('/'))
