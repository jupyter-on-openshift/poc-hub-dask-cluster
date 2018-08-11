import os
import time
import json
import threading
import string

from urllib.parse import quote

from flask import Flask, redirect, request, Response, abort
from flask import Blueprint, jsonify

from wrapt import decorator

from jupyterhub.services.auth import HubAuth

from kubernetes.client.rest import ApiException

from openshift.config import load_incluster_config
from openshift.client.api_client import ApiClient
from openshift.dynamic import DynamicClient, ResourceInstance
from openshift.watch import Watch

with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace') as fp:
    namespace = fp.read().strip()

load_incluster_config()

dyn_client = DynamicClient(ApiClient())

deployment_resource = dyn_client.resources.get(
        api_version='extensions/v1beta1', kind='Deployment')

service_resource = dyn_client.resources.get(
        api_version='v1', kind='Service')

pod_resource = dyn_client.resources.get(api_version='v1', kind='Pod')

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

def get_pods(name):
    dask_worker_name = '%s-worker-%s' % (dask_cluster_name, name)

    pods = pod_resource.get(namespace=namespace)

    details = []

    for pod in pods.items:
        if pod.metadata.labels['deployment'] == dask_worker_name:
            details.append((pod.metadata.name, pod.status.phase))

    return details

@controller.route('/pods', methods=['GET', 'OPTIONS', 'POST'])
@authenticated_user
def pods(user):
    return jsonify(get_pods(user['name']))

max_worker_replicas = int(os.environ.get('DASK_MAX_WORKER_REPLICAS', '0'))

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

    deployment_resource.scale.replace(namespace=namespace, body=body)

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

    deployment_resource.patch(namespace=namespace, name=name, body=body)

    return jsonify()

application.register_blueprint(controller, url_prefix=prefix.rstrip('/'))

worker_replicas = int(os.environ.get('DASK_WORKER_REPLICAS', 3))
worker_memory = os.environ.get('DASK_WORKER_MEMORY', '512Mi')
idle_timeout = int(os.environ.get('DASK_IDLE_CLUSTER_TIMEOUT', 600))

worker_deployment_template = string.Template("""
{
    "apiVersion": "extensions/v1beta1",
    "kind": "Deployment",
    "metadata": {
        "labels": {
            "app": "${application}",
            "component": "dask-worker",
            "dask-cluster": "${cluster}"
        },
        "name": "${name}",
        "namespace": "${namespace}",
        "ownerReferences": [
            {
                "apiVersion": "v1",
                "controller": true,
                "blockOwnerDeletion": false,
                "kind": "Service",
                "name": "${owner}",
                "uid": "${owner_uid}"
            }
        ]
    },
    "spec": {
        "replicas": ${replicas},
        "selector": {
            "matchLabels": {
                "app": "${application}",
                "deployment": "${name}"
            }
        },
        "strategy": {
            "type": "Recreate"
        },
        "template": {
            "metadata": {
                "annotations": {
                    "alpha.image.policy.openshift.io/resolve-names": "*"
                },
                "labels": {
                    "app": "${application}",
                    "deployment": "${name}"
                }
            },
            "spec": {
                "containers": [
                    {
                        "name": "worker",
                        "image": "${application}-notebook-img:latest",
                        "command": [
                            "start-daskworker.sh"
                        ],
                        "env": [
                            {
                                "name": "DASK_SCHEDULER_ADDRESS",
                                "value": "${scheduler}:8786"
                            }
                        ],
                        "ports": [
                            {
                                "containerPort": 8786,
                                "protocol": "TCP"
                            },
                            {
                                "containerPort": 8787,
                                "protocol": "TCP"
                            }
                        ],
                        "resources": {
                            "limits": {
                                "memory": "${memory}"
                            }
                        }
                    }
                ]
            }
        }
    }
}
""")

scheduler_deployment_template = string.Template("""
{
    "apiVersion": "extensions/v1beta1",
    "kind": "Deployment",
    "metadata": {
        "labels": {
            "app": "${application}",
            "component": "dask-scheduler",
            "dask-cluster": "${cluster}"
        },
        "name": "${name}",
        "namespace": "${namespace}",
        "ownerReferences": [
            {
                "apiVersion": "v1",
                "controller": true,
                "blockOwnerDeletion": false,
                "kind": "Service",
                "name": "${owner}",
                "uid": "${owner_uid}"
            }
        ]
    },
    "spec": {
        "replicas": 1,
        "selector": {
            "matchLabels": {
                "app": "${application}",
                "deployment": "${name}"
            }
        },
        "strategy": {
            "type": "Recreate"
        },
        "template": {
            "metadata": {
                "annotations": {
                    "alpha.image.policy.openshift.io/resolve-names": "*"
                },
                "labels": {
                    "app": "${application}",
                    "deployment": "${name}"
                }
            },
            "spec": {
                "containers": [
                    {
                        "name": "scheduler",
                        "image": "${application}-notebook-img:latest",
                        "command": [
                            "start-daskscheduler.sh"
                        ],
                        "ports": [
                            {
                                "containerPort": 8786,
                                "protocol": "TCP"
                            },
                            {
                                "containerPort": 8787,
                                "protocol": "TCP"
                            }
                        ],
                        "resources": {
                            "limits": {
                                "memory": "256Mi"
                            }
                        }
                    }
                ]
            }
        }
    }
}
""")

scheduler_service_template = string.Template("""
{
    "kind": "Service",
    "apiVersion": "v1",
    "metadata": {
        "namespace": "${namespace}",
        "name": "${name}",
        "labels": {
            "app": "${application}"
        }
    },
    "spec": {
        "ports": [
            {
                "name": "8786-tcp",
                "protocol": "TCP",
                "port": 8786,
                "targetPort": 8786
            },
            {
                "name": "8787-tcp",
                "protocol": "TCP",
                "port": 8787,
                "targetPort": 8787
            }
        ],
        "selector": {
            "app": "${application}",
            "deployment": "${name}"
        }
    }
}
""")

def create_cluster(name):
    scheduler_name = '%s-scheduler-%s' % (dask_cluster_name, name)

    worker_name = '%s-worker-%s' % (dask_cluster_name, name)

    try:
        text = scheduler_service_template.safe_substitute(
                namespace=namespace, name=scheduler_name,
                application=jupyterhub_name)

        body = json.loads(text)

        service = service_resource.create(namespace=namespace, body=body)

    except ApiException as e:
        if e.status != 409:
            print('ERROR: Error creating service. %s' % e)
            return

        else:
            try:
                service = service_resource.get(namespace=namespace,
                        name=scheduler_name)

            except Exception as e:
                print('ERROR: Error querying service. %s' % e)
                return

    except Exception as e:
        print('ERROR: Error creating service. %s' % e)
        return

    try:
        text = worker_deployment_template.safe_substitute(
                namespace=namespace, name=worker_name,
                application=jupyterhub_name, cluster=name,
                scheduler=scheduler_name, replicas=worker_replicas,
                memory=worker_memory, owner=service.metadata.name,
                owner_uid=service.metadata.uid)

        body = json.loads(text)

        deployment_resource.create(namespace=namespace, body=body)

    except ApiException as e:
        if e.status != 409:
            print('ERROR: Error creating worker deployment. %s' % e)

    except Exception as e:
        print('ERROR: Error creating worker deployment. %s' % e)

    try:
        text = scheduler_deployment_template.safe_substitute(
                namespace=namespace, name=scheduler_name,
                application=jupyterhub_name, cluster=name,
                owner=service.metadata.name, owner_uid=service.metadata.uid)

        body = json.loads(text)

        deployment_resource.create(namespace=namespace, body=body)

    except ApiException as e:
        if e.status != 409:
            print('ERROR: Error creating scheduler deployment. %s' % e)

    except Exception as e:
        print('ERROR: Error creating scheduler deployment. %s' % e)

def cluster_exists(name):
    try:
        scheduler_name = '%s-scheduler-%s' % (dask_cluster_name, name)

        deployment_resource.get(namespace=namespace, name=scheduler_name)

    except ApiException as e:
        if e.status == 404:
            return False

        print('ERROR: Error querying deployment config. %s' % e)

    else:
        return True

def new_notebook_added(pod):
    annotations = pod.metadata.annotations

    if annotations:
        name = annotations['jupyteronopenshift.org/dask-cluster']
        if name:
            found = cluster_exists(name)
            if found is not None and not found:
                create_cluster(name)

def monitor_pods():
    watcher = Watch()
    for item in watcher.stream(pod_resource.get,
            namespace=namespace, timeout_seconds=0,
            serialize=False):

        if item['type'] == 'ADDED':
            pod = ResourceInstance(pod_resource, item['object'])
            labels = pod.metadata.labels
            if labels['app'] == jupyterhub_name:
                if labels['component'] == 'singleuser-server':
                    new_notebook_added(pod)

thread1 = threading.Thread(target=monitor_pods)
thread1.set_daemon = True
thread1.start()

active_clusters = {}

def cull_clusters():

    while True:
        try:
            deployments = deployment_resource.get(namespace=namespace)

        except Exception as e:
            print('ERROR: Cannot query deployments.')

        else:
            for deployment in deployments.items:
                metadata = deployment.metadata
                if metadata:
                    labels = metadata.labels
                    if labels['component'] == 'dask-scheduler':
                        name = labels['dask-cluster']
                        if name:
                            active_clusters.setdefault(name, None)

        try:
            pods = pod_resource.get(namespace=namespace)

        except Exception as e:
            print('ERROR: Cannot query pods.')

        else:
            for pod in pods.items:
                metadata = pod.metadata
                if metadata:
                    annotations = metadata.annotations
                    name = annotations['jupyteronopenshift.org/dask-cluster']

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

                    # Only need to delete the service as deployments for
                    # the scheduler and workers have owner reference set
                    # to that for the service, so when delete the
                    # service, the deployments will also be deleted.

                    try:
                        delete_options = {
                            "kind": "DeleteOptions",
                            "apiVersion": "v1",
                            "propagationPolicy": "Foreground"
                        }

                        service_resource.delete(namespace=namespace,
                                name=scheduler_name, body=delete_options)

                    except ApiException as e:
                        if e.status != 404:
                            okay = False

                            print('ERROR: Could not delete cluster %s: %s' %
                                    (scheduler_name, e))

                    except Exception as e:
                        okay = False

                        print('ERROR: Could not delete cluster %s: %s' %
                                (scheduler_name, e))

                    if okay:
                        del active_clusters[name]

        time.sleep(30.0)

thread2 = threading.Thread(target=cull_clusters)
thread2.set_daemon = True
thread2.start()
