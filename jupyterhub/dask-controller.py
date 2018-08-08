import os
import time
import json
import threading
import string

from urllib.parse import quote

from flask import Flask, redirect, request, Response, abort
from flask import Blueprint, jsonify

from jupyterhub.services.auth import HubAuth

os.environ['KUBERNETES_SERVICE_HOST'] = 'openshift.default.svc.cluster.local'
os.environ['KUBERNETES_SERVICE_PORT'] = '443'

from openshift import config as oconfig
from openshift import client as oclient

from openshift.dynamic import DynamicClient

from openshift.client.models import (V1DeploymentConfig,
        V1DeploymentConfigSpec, V1DeploymentStrategy,
        V1DeploymentTriggerPolicy, V1DeploymentTriggerImageChangeParams)

from kubernetes import client as kclient, watch as kwatch

from kubernetes.client.models import (V1ObjectMeta,
        V1Service, V1ObjectReference, V1PodTemplateSpec, V1PodSpec,
        V1Container, V1ContainerPort, V1ResourceRequirements, V1EnvVar,
        V1ServiceSpec, V1ServicePort, V1DeleteOptions)

from kubernetes.client.rest import ApiException

from wrapt import decorator

with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace') as fp:
    namespace = fp.read().strip()

oconfig.load_incluster_config()

from kubernetes.client.api_client import ApiClient

dyn_client = DynamicClient(ApiClient())

corev1api = kclient.CoreV1Api()
appsopenshiftiov1api = oclient.AppsOpenshiftIoV1Api()

auth = HubAuth(api_token=os.environ['JUPYTERHUB_API_TOKEN'],
        cookie_cache_max_age=60)

jupyterhub_service_name = os.environ.get('JUPYTERHUB_SERVICE_NAME', '')
prefix = os.environ.get('JUPYTERHUB_SERVICE_PREFIX', '')

jupyterhub_name = os.environ.get('JUPYTERHUB_NAME', '')

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

pod_resource = dyn_client.resources.get(api_version='v1', kind='Pod')

def get_pods(name):
    dask_worker_name = '%s-worker-%s' % (dask_cluster_name, name)

    pods = pod_resource.get(namespace=namespace)

    details = []

    for pod in pods.items:
        if pod.metadata.labels['deploymentconfig'] == dask_worker_name:
            details.append((pod.metadata.name, pod.status.phase))

    return details

@controller.route('/pods', methods=['GET', 'OPTIONS', 'POST'])
@authenticated_user
def pods(user):
    return jsonify(get_pods(user['name']))

max_worker_replicas = int(os.environ.get('DASK_MAX_WORKER_REPLICAS', '0'))

deploymentconfig_resource = dyn_client.resources.get(
        api_version='apps.openshift.io/v1', kind='DeploymentConfig')

scale_template = string.Template("""
{
    "kind": "Scale",
    "apiVersion": "extensions/v1beta1",
    "metadata": {
        "namespace": "${namespace}",
	"name": "${name}"
    },
    "spec": {
	"replicas": ${replicas}
    }
}
""")

@controller.route('/scale', methods=['GET', 'OPTIONS', 'POST'])
@authenticated_user
def scale(user):
    replicas = request.args.get('replicas', None)

    if replicas is None:
        return jsonify()

    replicas = int(replicas)

    if max_worker_replicas > 0:
        replicas = min(replicas, max_worker_replicas)

    name = '%s-worker-%s' % (dask_cluster_name, user['name'])

    body = json.loads(scale_template.safe_substitute(namespace=namespace,
            name=name, replicas=replicas))

    deploymentconfig_resource.scale.replace(namespace=namespace, body=body)

    return jsonify()

restart_template = string.Template("""
{
    "spec": {
        "template": {
            "metadata": {
                "annotations": {
                    "dask-controller/restart": "${time}"
                }
            }
        }
    }
}
""")

@controller.route('/restart', methods=['GET', 'OPTIONS', 'POST'])
@authenticated_user
def restart(user):
    name = '%s-worker-%s' % (dask_cluster_name, user['name'])

    body = json.loads(restart_template.safe_substitute(time=time.time()))

    deploymentconfig_resource.patch(namespace=namespace, name=name, body=body)

    return jsonify()

application.register_blueprint(controller, url_prefix=prefix.rstrip('/'))

worker_replicas = int(os.environ.get('DASK_WORKER_REPLICAS', 3))
worker_memory = os.environ.get('DASK_WORKER_MEMORY', '512Mi')
idle_timeout = int(os.environ.get('DASK_IDLE_CLUSTER_TIMEOUT', 600))

def create_cluster(name):
    scheduler_name = '%s-scheduler-%s' % (dask_cluster_name, name)

    worker_name = '%s-worker-%s' % (dask_cluster_name, name)

    worker_deployment = V1DeploymentConfig(
        kind='DeploymentConfig',
        api_version='apps.openshift.io/v1',
        metadata=V1ObjectMeta(
            name=worker_name,
            namespace=namespace,
            labels={
                'app': jupyterhub_name,
                'component': 'dask-worker',
                'dask-cluster': name
            }
        ),
        spec=V1DeploymentConfigSpec(
            strategy=V1DeploymentStrategy(
                type='Recreate'
            ),
            triggers=[
                V1DeploymentTriggerPolicy(
                    type='ConfigChange'
                ),
                V1DeploymentTriggerPolicy(
                    type='ImageChange',
                    image_change_params=V1DeploymentTriggerImageChangeParams(
                        automatic=True,
                        container_names=['worker'],
                        _from=V1ObjectReference(
                            kind='ImageStreamTag',
                            name='%s-notebook-img:latest' % jupyterhub_name
                        )
                    )
                )
            ],
            replicas=worker_replicas,
            selector={
                'app': jupyterhub_name,
                'deploymentconfig': scheduler_name
            },
            template=V1PodTemplateSpec(
                metadata = V1ObjectMeta(
                    labels={
                        'app': jupyterhub_name,
                        'deploymentconfig': scheduler_name
                    }
                ),
                spec=V1PodSpec(
                    containers=[
                        V1Container(
                            name='worker',
                            image='%s-notebook-img:latest' % jupyterhub_name,
                            command=['start-daskworker.sh'],
                            ports=[
                                V1ContainerPort(
                                    container_port=8786,
                                    protocol='TCP'
                                ),
                                V1ContainerPort(
                                    container_port=8787,
                                    protocol='TCP'
                                )
                            ],
                            resources=V1ResourceRequirements(
                                limits={'memory':worker_memory}
                            ),
                            env=[
                                V1EnvVar(
                                    name='DASK_SCHEDULER_ADDRESS',
                                    value='%s:8786' % scheduler_name
                                )
                            ]
                        )
                    ]
                )
            )
        )
    )

    scheduler_deployment = V1DeploymentConfig(
        kind='DeploymentConfig',
        api_version='apps.openshift.io/v1',
        metadata=V1ObjectMeta(
            name=scheduler_name,
            namespace=namespace,
            labels={
                'app': jupyterhub_name,
                'component': 'dask-scheduler',
                'dask-cluster': name
            }
        ),
        spec=V1DeploymentConfigSpec(
            strategy=V1DeploymentStrategy(
                type='Recreate'
            ),
            triggers=[
                V1DeploymentTriggerPolicy(
                    type='ConfigChange'
                ),
                V1DeploymentTriggerPolicy(
                    type='ImageChange',
                    image_change_params=V1DeploymentTriggerImageChangeParams(
                        automatic=True,
                        container_names=['scheduler'],
                        _from=V1ObjectReference(
                            kind='ImageStreamTag',
                            name='%s-notebook-img:latest' % jupyterhub_name
                        )
                    )
                )
            ],
            replicas=1,
            selector={
                'app': jupyterhub_name,
                'deploymentconfig': scheduler_name
            },
            template=V1PodTemplateSpec(
                metadata = V1ObjectMeta(
                    labels={
                        'app': jupyterhub_name,
                        'deploymentconfig': scheduler_name
                    }
                ),
                spec=V1PodSpec(
                    containers=[
                        V1Container(
                            name='scheduler',
                            image='%s-notebook-img:latest' % jupyterhub_name,
                            command=['start-daskscheduler.sh'],
                            ports=[
                                V1ContainerPort(
                                    container_port=8786,
                                    protocol='TCP'
                                ),
                                V1ContainerPort(
                                    container_port=8787,
                                    protocol='TCP'
                                )
                            ],
                            resources=V1ResourceRequirements(
                                limits={'memory':'256Mi'}
                            ),
                            env=[
#                                V1EnvVar(
#                                    name='DASK_SCHEDULER_ARGS',
#                                    value='--bokeh-prefix /services/dask-monitor'
#                                )
                            ]
                        )
                    ]
                )
            )
        )
    )

    scheduler_service = V1Service(
        kind='Service',
        api_version='v1',
        metadata = V1ObjectMeta(
            name=scheduler_name,
            namespace=namespace,
            labels={'app': jupyterhub_name}
        ),
        spec=V1ServiceSpec(
            ports=[
                V1ServicePort(
                    name='8786-tcp',
                    protocol='TCP',
                    port=8786,
                    target_port=8786
                ),
                V1ServicePort(
                    name='8787-tcp',
                    protocol='TCP',
                    port=8787,
                    target_port=8787
                )
            ],
            selector={
                'app': jupyterhub_name,
                'deploymentconfig': scheduler_name
            }
        )
    )

    try:
        appsopenshiftiov1api.create_namespaced_deployment_config(
                namespace, worker_deployment)

    except ApiException as e:
        if e.status != 409:
            print('ERROR: Error creating worker deployment. %s' % e)

    except Exception as e:
        print('ERROR: Error creating worker deployment. %s' % e)

    try:
        appsopenshiftiov1api.create_namespaced_deployment_config(
                namespace, scheduler_deployment)

    except ApiException as e:
        if e.status != 409:
            print('ERROR: Error creating scheduler deployment. %s' % e)

    except Exception as e:
        print('ERROR: Error creating scheduler deployment. %s' % e)

    try:
        corev1api.create_namespaced_service(
                namespace, scheduler_service)

    except ApiException as e:
        if e.status != 409:
            print('ERROR: Error creating service. %s' % e)

    except Exception as e:
        print('ERROR: Error creating service. %s' % e)

def cluster_exists(name):
    try:
        scheduler_name = '%s-scheduler-%s' % (dask_cluster_name, name)

        dc = appsopenshiftiov1api.read_namespaced_deployment_config(
                scheduler_name, namespace)

    except ApiException as e:
        if e.status == 404:
            return False

        print('ERROR: Error querying deployment config. %s' % e)

    else:
        return True

def new_notebook_added(pod):
    name = pod.metadata.annotations.get('jupyteronopenshift.org/dask-cluster')
    if name:
        found = cluster_exists(name)
        if found is not None and not found:
            create_cluster(name)

def monitor_pods():
    watcher = kwatch.Watch()
    for item in watcher.stream(corev1api.list_namespaced_pod,
            namespace=namespace, timeout_seconds=0):

        if item['type'] == 'ADDED':
            pod = item['object']
            labels = pod.metadata.labels
            if labels.get('app') == jupyterhub_name:
                if labels.get('component') == 'singleuser-server':
                    new_notebook_added(pod)

thread1 = threading.Thread(target=monitor_pods)
thread1.set_daemon = True
thread1.start()

active_clusters = {}

def cull_clusters():

    while True:
        try:
            deployments = appsopenshiftiov1api.list_namespaced_deployment_config(
                    namespace)

        except Exception as e:
            print('ERROR: Cannot query deployments.')

        else:
            for deployment in deployments.items:
                labels = deployment.metadata.labels
                if labels.get('component') == 'dask-scheduler':
                    name = labels.get('dask-cluster', '')
                    active_clusters.setdefault(name, None)

        try:
            pods = corev1api.list_namespaced_pod(namespace)

        except Exception as e:
            print('ERROR: Cannot query pods.')

        else:
            for pod in pods.items:
                annotations = pod.metadata.annotations
                name = annotations.get('jupyteronopenshift.org/dask-cluster')

                if name and name in active_clusters:
                    del active_clusters[name]

        now = time.time()

        for name, timestamp in list(active_clusters.items()):
            if timestamp is None:
                active_clusters[name] = now

            else:
                if now - timestamp > idle_timeout:
                    print('INFO: deleting dask cluster %s.' % name)

                    okay = True

                    scheduler_name = '%s-scheduler-%s' % (dask_cluster_name, name)

                    try:
                        appsopenshiftiov1api.delete_namespaced_deployment_config(
                                scheduler_name, namespace, V1DeleteOptions())

                    except ApiException as e:
                        if e.status != 404:
                            okay = False

                            print('ERROR: Could not delete scheduler %s: %s' %
                                    (scheduler_name, e))

                    except Exception as e:
                        okay = False

                        print('ERROR: Could not delete scheduler %s: %s' %
                                (scheduler_name, e))

                    try:
                        corev1api.delete_namespaced_service(
                                scheduler_name, namespace, V1DeleteOptions())

                    except ApiException as e:
                        if e.status != 404:
                            okay = False

                            print('ERROR: Could not delete service %s: %s' %
                                    (scheduler_name, e))

                    except Exception as e:
                        okay = False

                        print('ERROR: Could not delete service %s: %s' %
                                (scheduler_name, e))

                    worker_name = '%s-worker-%s' % (dask_cluster_name, name)

                    try:
                        appsopenshiftiov1api.delete_namespaced_deployment_config(
                                worker_name, namespace, V1DeleteOptions())

                    except ApiException as e:
                        if e.status != 404:
                            okay = False

                            print('ERROR: Could not delete worker %s: %s' %
                                    (worker_name, e))

                    except Exception as e:
                        okay = False

                        print('ERROR: Could not delete worker %s: %s' %
                                (worker_name, e))

                    if okay:
                        del active_clusters[name]

        time.sleep(30.0)

thread2 = threading.Thread(target=cull_clusters)
thread2.set_daemon = True
thread2.start()
