Always clean cloud resources that are no longer necessary; they could be costly!

See all controllers in your machine with

```{terminal}
:copy:

juju controllers

Controller         Model         User   Access     Cloud/Region                Models  Nodes    HA  Version
<controller-name>  <model-name>  admin  superuser  <cloud-name>/<region-name>  1       1      none  3.6.1
```

The following command will destroy the Juju controller and remove the cloud instance - meaning **all your data will be permanently removed**:

```{terminal}
:copy:

juju destroy-controller <controller-name> --destroy-all-models --destroy-storage --force
```