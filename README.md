JupyterHub (KeyCloak+Dask)
==========================

This repository contains a sample application for deploying JupyterHub as a means to provide Jupyter notebooks to multiple users. Authentication of users is managed using KeyCloak. Each user will be provided with their own Dask cluster for use with the sample notebooks.

Deploying the application
-------------------------

To deploy the sample application, you can run:

```
oc new-app https://raw.githubusercontent.com/jupyter-on-openshift/poc-hub-dask-cluster/master/templates/jupyterhub.json
```

This will create all the required builds and deployments from the one template.

If desired, you can instead load the template, with instantiation of the template done as a separate step from the command line or using the OpenShift web console.

Resource requirements
---------------------

If deploying to an OpenShift environment that enforces quotas, you must have a memory quota for terminating workloads (pods) of 3GiB so that builds can be run. For one user, you will need 6GiB of quota for terminating workloads (pods). Each additional user requires 3.25GiB.

A user can scale up the number of Dask workers from the default of 3 up to a maximum of 5, from the JupyterHub control panel. They will need 1GiB for each additional worker.

Registering a user
------------------

KeyCloak will be deployed, with JupyterHub and KeyCloak automatically configured to handle authentication of users. No users are setup in advance, but users can register themselves by clicking on the _Register_ link on the login page.
