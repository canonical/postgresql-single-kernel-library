(troubleshooting-overview)=
# How to troubleshoot
{{vm_k8s}}

This page goes over some recommended tools and approaches to troubleshooting the charm.

```{dropdown} To avoid service disruptions:
:open:
:class-container: dropdown-tip
:icon: light-bulb
:class-title: sd-font-weight-normal

Make sure your troubleshooting activity does not interfere with the operator itself.

Do not manage users, credentials, databases, and schema directly. This could cause a {term}`split-brain scenario`.

Avoid restarting services directly. If you see the problem with a unit, consider {ref}`removing the failing unit and adding a new unit <scale-cluster>` to recover the cluster state.
```

## Check charm status

Before anything, always run `juju status` and check the {ref}`recommended fixes <charm-statuses>`. This alone may already solve your issue.

## Check logs

Always check the logs before troubleshooting further.

Start by checking the [Juju logs](https://juju.is/docs/juju/log):

```shell
juju debug-log --replay --tail
```

````{tab-set}
```{tab-item} VM
:sync: vm

Check the PostgreSQL and Patroni logs located inside the snap:

    ls -la /var/snap/charmed-postgresql/common/var/log/*
```
```{tab-item} K8s
:sync: k8s

Check the PostgreSQL and Patroni logs located in the `workload` container:

    ls -la /var/lib/pg/logs/16/main/*
```
````

For more detailed information about the different types of logs, see: {ref}`logs`.

## Check processes running inside the charm

`````{tab-set}
````{tab-item} VM
:sync: vm

We recommend first reading through the {ref}`architecture` page to become familiar with snap content, operator building blocks, and running Juju units.

### Check snap services are running

To enter the unit, use:

```{terminal}
:copy:

juju ssh postgresql/0 bash
```

Make sure the `charmed-postgresql` snap is installed and functional:

```{terminal}
:copy:
:user: ubuntu
:host: juju-fd7874-0

sudo snap list charmed-postgresql

Name                Version  Rev  Tracking       Publisher        Notes
charmed-postgresql  14.9     70   latest/stable  dataplatformbot  held
```

From here you can make sure all snap (`systemd`) services are running:

```{terminal}
:copy:
:user: ubuntu
:host: juju-fd7874-0

sudo snap services

Service                                          Startup   Current   Notes
charmed-postgresql.patroni                       enabled   active    -
charmed-postgresql.pgbackrest-service            enabled   active    -
charmed-postgresql.prometheus-postgres-exporter  enabled   active    -
charmed-postgresql.pgbackrest-exporter           enabled   active    -
```

```{terminal}
:copy:
:user: ubuntu
:host: juju-fd7874-0

systemctl --failed
...
0 loaded units listed.
```

```{terminal}
:copy:
:user: ubuntu
:host: juju-fd7874-0

ps auxww

USER         PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND
root           1  0.4  0.0 167364 12716 ?        Ss   21:40   0:02 /sbin/init
root          59  0.0  0.0  64596 20828 ?        Ss   21:40   0:00 /lib/systemd/systemd-journald
root         112  0.0  0.0  11088  5740 ?        Ss   21:40   0:00 /lib/systemd/systemd-udevd
root         115  0.3  0.0   4832  1816 ?        Ss   21:40   0:01 snapfuse /var/lib/snapd/snaps/core22_864.snap /snap/core22/864 -o ro,nodev,allow_other,suid
root         116  0.2  0.0   4896  1880 ?        Ss   21:40   0:01 snapfuse /var/lib/snapd/snaps/charmed-postgresql_70.snap /snap/charmed-postgresql/70 -o ro,nodev,allow_other,suid
root         117  0.0  0.0   4748  1644 ?        Ss   21:40   0:00 snapfuse /var/lib/snapd/snaps/core20_2015.snap /snap/core20/2015 -o ro,nodev,allow_other,suid
root         119  0.0  0.0   4692  1600 ?        Ss   21:40   0:00 snapfuse /var/lib/snapd/snaps/lxd_24322.snap /snap/lxd/24322 -o ro,nodev,allow_other,suid
root         120  0.6  0.0   4768  1840 ?        Ss   21:40   0:04 snapfuse /var/lib/snapd/snaps/snapd_19993.snap /snap/snapd/19993 -o ro,nodev,allow_other,suid
systemd+     225  0.0  0.0  16116  8100 ?        Ss   21:40   0:00 /lib/systemd/systemd-networkd
systemd+     227  0.0  0.0  25528 12664 ?        Ss   21:40   0:00 /lib/systemd/systemd-resolved
root         241  0.0  0.0   7284  2792 ?        Ss   21:40   0:00 /usr/sbin/cron -f -P
message+     243  0.0  0.0   8668  4916 ?        Ss   21:40   0:00 @dbus-daemon --system --address=systemd: --nofork --nopidfile --systemd-activation --syslog-only
root         247  0.0  0.0  33084 18792 ?        Ss   21:40   0:00 /usr/bin/python3 /usr/bin/networkd-dispatcher --run-startup-triggers
syslog       248  0.0  0.0 152764  4748 ?        Ssl  21:40   0:00 /usr/sbin/rsyslogd -n -iNONE
snap_da+     250  0.0  0.0 1303900 10216 ?       Ssl  21:40   0:00 /snap/charmed-postgresql/70/usr/bin/prometheus-postgres-exporter
root         254  0.0  0.0  15312  7456 ?        Ss   21:40   0:00 /lib/systemd/systemd-logind
root         281  0.0  0.0   7760  3508 ?        Ss   21:40   0:00 bash /etc/systemd/system/jujud-machine-0-exec-start.sh
root         294  0.0  0.0   6216  1064 pts/0    Ss+  21:40   0:00 /sbin/agetty -o -p -- \u --noclear --keep-baud console 115200,38400,9600 vt220
root         296  0.0  0.0  15420  9240 ?        Ss   21:40   0:00 sshd: /usr/sbin/sshd -D [listener] 0 of 10-100 startups
root         301  2.2  0.2 895540 97552 ?        Sl   21:40   0:13 /var/lib/juju/tools/machine-0/jujud machine --data-dir /var/lib/juju --machine-id 0 --debug
root         335  0.0  0.0 110084 21336 ?        Ssl  21:40   0:00 /usr/bin/python3 /usr/share/unattended-upgrades/unattended-upgrade-shutdown --wait-for-signal
root         418  0.0  0.0 235452  8128 ?        Ssl  21:40   0:00 /usr/libexec/polkitd --no-debug
root         772  0.4  0.0   4764  1780 ?        Ss   21:40   0:02 snapfuse /var/lib/snapd/snaps/snapd_20092.snap /snap/snapd/20092 -o ro,nodev,allow_other,suid
root         850  0.2  0.1 2058980 33536 ?       Ssl  21:40   0:01 /usr/lib/snapd/snapd
root        1587  0.0  0.0   4780  3264 ?        Ss   21:40   0:00 /bin/bash /snap/charmed-postgresql/70/start-patroni.sh
snap_da+    1615  1.1  0.1 490500 39308 ?        Sl   21:40   0:06 python3 /snap/charmed-postgresql/70/usr/bin/patroni /var/snap/charmed-postgresql/70/etc/patroni/patroni.yaml
snap_da+    2582  0.0  0.0 215816 30076 ?        S    21:41   0:00 /snap/charmed-postgresql/current/usr/lib/postgresql/14/bin/postgres -D /var/snap/charmed-postgresql/common/var/lib/postgresql --config-file=/var/snap/charmed-postgresql/common/var/lib/postgresql/postgresql.conf --listen_addresses=10.47.228.200 --port=5432 --cluster_name=postgresql --wal_level=logical --hot_standby=on --max_connections=100 --max_wal_senders=10 --max_prepared_transactions=0 --max_locks_per_transaction=64 --track_commit_timestamp=off --max_replication_slots=10 --max_worker_processes=8 --wal_log_hints=on
snap_da+    2808  0.0  0.0 215816 10704 ?        Ss   21:41   0:00 postgres: postgresql: checkpointer
snap_da+    2810  0.0  0.0 215816 10496 ?        Ss   21:41   0:00 postgres: postgresql: background writer
snap_da+    2811  0.0  0.0  70540  8804 ?        Ss   21:41   0:00 postgres: postgresql: stats collector
snap_da+    2840  0.0  0.0 217980 21184 ?        Ss   21:41   0:00 postgres: postgresql: operator postgres 10.47.228.200(36138) idle
snap_da+    2947  0.0  0.0 216716 14736 ?        Ss   21:41   0:00 postgres: postgresql: walsender replication 10.47.228.241(45254) streaming 0/A002FA8
snap_da+    2952  0.0  0.0 215816 13140 ?        Ss   21:41   0:00 postgres: postgresql: walwriter
snap_da+    2953  0.0  0.0 216424 10848 ?        Ss   21:41   0:00 postgres: postgresql: autovacuum launcher
snap_da+    2954  0.0  0.0 215816  9132 ?        Ss   21:41   0:00 postgres: postgresql: archiver last was 00000001000000000000000A.partial
snap_da+    2955  0.0  0.0 216260  9516 ?        Ss   21:41   0:00 postgres: postgresql: logical replication launcher
snap_da+    6556  0.0  0.0 216780 14780 ?        Ss   21:42   0:00 postgres: postgresql: walsender replication 10.47.228.164(48482) streaming 0/A002FA8
root        6799  0.0  0.0  39900 31164 ?        S    21:42   0:00 /usr/bin/python3 src/cluster_topology_observer.py https://10.47.228.200:8008 /var/snap/charmed-postgresql/current/etc/patroni/ca.pem /usr/bin/juju-run postgresql/0 /var/lib/juju/agents/unit-postgresql-0/charm
root        9831  0.0  0.0   4780  3204 ?        Ss   21:46   0:00 /bin/bash /snap/charmed-postgresql/70/start-pgbackrest.sh
snap_da+    9859  0.0  0.0  56152 13584 ?        S    21:46   0:00 /snap/charmed-postgresql/70/usr/bin/pgbackrest server --config=/var/snap/charmed-postgresql/70/etc/pgbackrest/pgbackrest.conf
root       10168  0.0  0.0  16908 10836 ?        Ss   21:47   0:00 sshd: ubuntu [priv]
ubuntu     10171  0.0  0.0  17056  9628 ?        Ss   21:47   0:00 /lib/systemd/systemd --user
ubuntu     10172  0.0  0.0 170148  4728 ?        S    21:47   0:00 (sd-pam)
ubuntu     10234  0.0  0.0  17208  7944 ?        R    21:47   0:00 sshd: ubuntu@pts/1
```

The list of running Pebble services will depend on configured {ref}`COS integration <enable-monitoring>` and {ref}`backup <create-a-backup>` functionality.

The snap service `charmed-postgresql.patroni` must always be active and currently running (the Linux processes `snapd`, `patroni` and `postgres`).

### Access PostgreSQL with `psql`

Access PostgreSQL with the `psql` CLI tool and continue troubleshooting your database-related issues from here.

```shell
juju show-unit postgresql/0 | awk '/private-address:/{print $2;exit}'
juju secrets # to find secret ID
juju show-secret --reveal <secret ID> | grep operator
```

{{seealso}} {ref}`manage-passwords`

````
````{tab-item} K8s
:sync: k8s

We recommend first reading through the {ref}`architecture` page to become familiar with `charm` and `workload` containers.

Make sure both containers are `Running` and `Ready` to continue troubleshooting inside the charm.

To describe the running pod, use the following command (where `0` is a Juju unit id):

```{terminal}
:copy:

kubectl describe pod postgresql-k8s-0 -n <juju_model_name>

...
Containers:
  charm:
    ...
    Image:          jujusolutions/charm-base:ubuntu-22.04
    State:          Running
    Ready:          True
    Restart Count:  0
    ...
  postgresql:
    ...
    Image:          registry.jujucharms.com/charm/kotcfrohea62xreenq1q75n1lyspke0qkurhk/postgresql-image@something
    State:          Running
    Ready:          True
    Restart Count:  0
    ...
```

### Check Pebble processes in the charm container

To enter the charm container, use:

```{terminal}
:copy:

juju ssh postgresql-k8s/0 bash
```

Here you can make sure pebble is running.

The Pebble plan is:

```{terminal}
:copy:
:user: root
:host: postgresql-k8s-0
:dir: /var/lib/juju

/charm/bin/pebble services

Service          Startup  Current  Since
container-agent  enabled  active   today at 12:29 UTC
```
```{terminal}
:copy:
:user: root
:host: postgresql-k8s-0
:dir: /var/lib/juju

ps auxww

USER         PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND
root           1  0.0  0.0 718264 10876 ?        Ssl  12:29   0:00 /charm/bin/pebble run --http :38812 --verbose
root          15  0.6  0.1 778628 59148 ?        Sl   12:29   0:03 /charm/bin/containeragent unit --data-dir /var/lib/juju --append-env PATH=$PATH:/charm/bin --show-log --charm-modified-version 0
```

### Check PostgreSQL services the workload container

To enter the workload container, use:

```{terminal}
:copy:
juju ssh --container postgresql postgresql-k8s/0 bash
```

You can check the list of running processes and Pebble plan:

```{terminal}
:copy:
:user: root
:host: postgresql-k8s-0

/charm/bin/pebble services

Service            Startup   Current   Since
metrics_server     enabled   active    today at 12:30 UTC
pgbackrest server  disabled  inactive  -
postgresql         enabled   active    today at 12:29 UTC
```
```{terminal}
:copy:
:user: root
:host: postgresql-k8s-0

ps auxww

USER         PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND
root           1  0.0  0.0 718264 10916 ?        Ssl  12:29   0:00 /charm/bin/pebble run --create-dirs --hold --http :38813 --verbose
postgres      14  0.1  0.1 565020 39412 ?        Sl   12:29   0:01 python3 /usr/bin/patroni /var/lib/pg/data/patroni.yml
postgres      30  0.0  0.0 1082704 9076 ?        Sl   12:30   0:00 /usr/bin/prometheus-postgres-exporter
postgres      48  0.0  0.0 215488 28912 ?        S    12:30   0:00 /usr/lib/postgresql/16/bin/postgres -D /var/lib/pg/data/16/main --config-file=/var/lib/pg/data/16/main/postgresql.conf --listen_addresses=0.0.0.0 --port=5432 --cluster_name=patroni-postgresql-k8s --wal_level=logical --hot_standby=on --max_connections=100 --max_wal_senders=10 --max_prepared_transactions=0 --max_locks_per_transaction=64 --track_commit_timestamp=off --max_replication_slots=10 --max_worker_processes=8 --wal_log_hints=on
postgres      50  0.0  0.0  70080  7488 ?        Ss   12:30   0:00 postgres: patroni-postgresql-k8s: logger
postgres      52  0.0  0.0 215592  9136 ?        Ss   12:30   0:00 postgres: patroni-postgresql-k8s: checkpointer
postgres      53  0.0  0.0 215604  9632 ?        Ss   12:30   0:00 postgres: patroni-postgresql-k8s: background writer
postgres      54  0.0  0.0 215488 12208 ?        Ss   12:30   0:00 postgres: patroni-postgresql-k8s: walwriter
postgres      55  0.0  0.0 216052 10928 ?        Ss   12:30   0:00 postgres: patroni-postgresql-k8s: autovacuum launcher
postgres      56  0.0  0.0 215488  8420 ?        Ss   12:30   0:00 postgres: patroni-postgresql-k8s: archiver
postgres      57  0.0  0.0  70196  8384 ?        Ss   12:30   0:00 postgres: patroni-postgresql-k8s: stats collector
postgres      58  0.0  0.0 216032 10476 ?        Ss   12:30   0:00 postgres: patroni-postgresql-k8s: logical replication launcher
postgres      60  0.0  0.0 218060 21428 ?        Ss   12:30   0:00 postgres: patroni-postgresql-k8s: operator postgres 127.0.0.1(59528) idle
```

The list of running Pebble services will depend on configured {ref}`COS integration <enable-monitoring>` and {ref}`backup <create-a-backup>` functionality.

Pebble and its service `postgresql` must always be enabled and currently running (the Linux processes `pebble`, `patroni` and `postgres`).

### Access PostgreSQL with `psql`

Access PostgreSQL with the `psql` CLI tool and continue troubleshooting your database-related issues from here.

```{terminal}
:copy:

juju run postgresql-k8s/leader get-password username=operator

password: 3wMQ1jzfuERvTEds
```
```{terminal}
:copy:

juju ssh --container postgresql postgresql-k8s/0 bash

> psql -h 127.0.0.1 -U operator -d postgres -W
> Password for user operator: 3wMQ1jzfuERvTEds
> postgres=# \l
> postgres  | operator | UTF8     | C       | C.UTF-8 | operator=CTc/operator  +
>           |          |          |         |         | backup=CTc/operator    +
...
```
````
`````

## Install extra software

We do not recommend in stalling additional software, as it may affect stability and create anomalies that are hard to troubleshoot.

However, if extra troubleshooting software is necessary, install from trusted sources.

`````{tab-set}
````{tab-item} VM
:sync: vm

```{terminal}
:copy:
:user: ubuntu
:host: juju-fd7874-0

sudo apt update && sudo apt install gdb

...
Setting up gdb (12.1-0ubuntu1~22.04) ...
```
````

````{tab-item} K8s
:sync: k8s

```{terminal}
:copy:
:user: root
:host: postgresql-k8s-0

apt update && apt install less

...
Setting up less (590-1ubuntu0.22.04.1) ...
```
````
`````

```{tip}
Always remove manually installed components at the end of troubleshooting. Keep the house clean!
```

