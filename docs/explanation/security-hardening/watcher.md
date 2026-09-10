---
myst:
  html_meta:
    description: "Security documentation for the PostgreSQL watcher charm: trust boundaries, security by design, cryptography, hardening, inherent risks, logging, decommissioning, lifecycle, and vulnerability reporting."
---

(security-hardening-watcher)=
# Watcher security
{{vm}}

This page documents the security posture of the PostgreSQL watcher charm (`postgresql-watcher`) — how it is designed to be secure, how to operate it securely, which risks are inherent to its function, and how to report vulnerabilities. How the watcher participates in a cluster (deployment, integration, voting behaviour) is covered in [Stereo mode](explanation-stereo-mode); this page does not restate that.

Scope note: the watcher is packaging-layer software — a Juju charm operating a Raft voting service. It holds no user data and performs no cryptographic operations of its own; its security story is almost entirely *composition* — Juju, the {spellexception}`charmed-postgresql` snap, and the PostgreSQL cluster it votes in. Where a requirement is satisfied by an upstream component, this page says so and links out rather than duplicating. Environment-level hardening (clouds, Juju, credentials) applies to watcher hosts exactly as to PostgreSQL units and is covered in the [Security hardening overview](security-hardening-overview).

```{note}
The watcher is available for Charmed PostgreSQL 16 (VM substrate) only.
```

## Product architecture

The watcher provides a **third Raft vote for 2-node Charmed PostgreSQL clusters** (stereo mode). A two-member cluster cannot tolerate partition by itself; the watcher runs Patroni's own `patroni-raft-controller` as a third, data-less voting member so quorum decisions survive the loss of one PostgreSQL unit.

Deployment shape:

- The watcher installs the same pinned `charmed-postgresql` snap used by Charmed PostgreSQL, but runs **only** `patroni-raft-controller` from it — no PostgreSQL process, no data directory with user data, no client listener.
- One watcher unit is permitted; the charm blocks itself if a second unit is added.
- For each relation to a Charmed PostgreSQL application, the watcher instantiates a separate Raft controller (own port, own data directory, own systemd service instance).

Trust boundaries (mirrored from the project's internal threat model):

| Boundary | What crosses it | Protection |
|---|---|---|
| Watcher ↔ PostgreSQL Raft (TCP, port 2222 on both; 2223+ only for a second simultaneous relation) | Raft consensus messages | Shared Raft password, distributed out-of-band via Juju secrets; cluster-internal network only |
| Watcher ↔ PostgreSQL health endpoint | Patroni REST status polls; PostgreSQL health connection by the `watcher` user | Cluster-internal network; TLS against the cluster CA <!-- TODO(SEC0030 review): confirm exact TLS usage for the psycopg2 connection --> |
| Juju → watcher charm | Relation data, config, secrets, events | Juju model access control; secrets exposed only through Juju's secrets API |
| Watcher relation (PostgreSQL charm → watcher charm) | Raft partner addresses, cluster name, CA bundle, secret ID, status/address updates | Juju relation data is readable only by applications in the relation; the Raft password itself is NOT in relation data — only its secret ID |

## Security by design

- **Least privilege**: the watcher holds no user data and no superuser credential. Its PostgreSQL identity is a dedicated `watcher` user whose password arrives via Juju secret; its Raft identity is a membership password. It runs one systemd service and writes only under the snap's common data path and `/etc/systemd`.
- **Attack-surface minimisation**: no exposed client ports beyond the Raft listener; no actions that mutate the database; optional relation (the charm idles harmlessly unrelated). Adding the watcher to an odd-sized cluster is refused with a warning, because an even Raft membership degrades partition tolerance.
- **Fail-safe defaults**: the charm waits (Blocked/Waiting) until a relation exists; Raft membership is only configured after the password and partner addresses are present; the `production` profile blocks deployment when the watcher shares an availability zone with a PostgreSQL unit (correlated-failure protection).
- **Supply-chain control**: the snap is installed at a revision pinned in the charm release and **held** against auto-refresh; refreshes go through a coordinated rolling process with pre-refresh checks (`pre-refresh-check` action) rather than in-place auto-update. The charm repository uses branch protection and automated dependency management (Renovate, including vulnerability alerts).
- **No security by obscurity**: the design is documented here and in the public charm source; nothing relies on secrecy of the implementation.

## Cryptography in the product

**A. Overall use.** The watcher performs no cryptographic operations itself. Cryptography in a deployment containing a watcher comes from three places: Juju (secret storage and transport of the Raft and watcher passwords), the `charmed-postgresql` snap (OpenSSL/PostgreSQL for any TLS-protected connections), and the Raft membership password check.

**B. Cryptographic technology used by the product.** The only authentication mechanism the watcher itself exercises is the **Raft shared-password check** (membership authentication in Patroni's Raft implementation — see the Patroni/PySyncObj documentation for the primitive). No algorithms or key material are generated, negotiated, or stored by watcher code. <!-- TODO(SEC0030 review): characterise the PySyncObj password check and link the upstream reference -->

**C. Cryptographic technology exposed to users.** None. The watcher exposes no TLS endpoints, no certificate operations, and no user-facing cryptographic configuration. TLS for PostgreSQL client connections is a Charmed PostgreSQL feature (see [Enable TLS](enable-tls) and {ref}`Cryptography <cryptography>`) and is unaffected by adding a watcher.

**D. Packages providing cryptographic functionality.** All cryptographic functionality is inherited from: the Ubuntu archive (Python 3.12 runtime, OpenSSL inside the `charmed-postgresql` snap), the `charmed-postgresql` snap itself (Canonical-built, from canonical/charmed-postgresql-snap), and Python libraries from PyPI pinned in `poetry.lock` — notably `cryptography` (a library dependency of the platform libraries, not invoked by watcher code) and `pysyncobj`. Third-party packages come from PyPI; pinned versions are visible in the repository's `poetry.lock`.

**E. Encryption of data in transit and at rest.**

- *In transit*: Raft consensus traffic is **not TLS-encrypted**; it is protected by the shared membership password and by running on the cluster-internal network. If your security posture requires encryption for this traffic, do not deploy the watcher; a 3-unit PostgreSQL cluster is the alternative that removes the need for it. <!-- TODO(SEC0030 review): confirm with security engineering that this is the correct guidance --> The watcher's health-check connection to PostgreSQL uses TLS verified against the cluster CA <!-- TODO(SEC0030 review): confirm the CA bundle is used for the health-check TLS connection -->. Passwords never traverse relation data in plaintext — they travel as Juju secrets.
- *At rest*: the Raft configuration file (containing the Raft password in plaintext) and any CA bundle are written with `0600` permissions under `/var/snap/charmed-postgresql/common/watcher-raft/`, readable only by root. No other sensitive data is persisted. Full-disk encryption of the host is the user-side control if the deployment's threat model requires it (see Hardening guidelines below).

## Configuring and operating the product securely

The watcher ships with conservative defaults; the sections below cover what it does out of the box, which settings to review, and which risks the operator owns.

### Security by default

- The snap is pinned to a revision and held; it does not auto-update.
- The charm refuses to run more than one unit.
- The charm does not join any Raft cluster until the relation provides password and partner addresses; it never generates or guesses credentials.
- In the default `production` profile, deployment is blocked when the watcher shares an availability zone with a PostgreSQL unit.
- Credential rotation: rotating the Raft password is performed by the Charmed PostgreSQL side of the relation (new secret revision → `secret-changed` → watcher reconfigures) <!-- TODO(SEC0030 review): confirm the credential-rotation flow -->. Juju secret access is scoped to the model.

### Hardening guidelines

| Control | Guidance | Effect |
|---|---|---|
| Availability-zone separation (default in `profile=production`) | Deploy the watcher in a distinct AZ from all PostgreSQL units it watches | Prevents one AZ failure from removing the watcher vote and a PostgreSQL unit simultaneously |
| Model access | Grant Juju model access only to operators who need it; relation data and secrets are model-scoped | Limits who can read topology or add a rogue relation |
| Host access | Restrict SSH/root access to the watcher host as for any Juju-managed machine (see the [Security hardening overview](security-hardening-overview)) | The Raft password is readable on the host by root (0600 file); host compromise defeats it |
| Profile | Keep `profile=production` for production deployments | Enables the AZ blocking and production resource tuning |
| File permissions | Do not relax permissions on `/var/snap/charmed-postgresql/common/watcher-raft/` | The Raft password sits in plaintext in that tree (0600) |

### Risks inherent to product functions and recommended controls

These risks cannot be mitigated by Canonical without removing the watcher's function; they must be accepted or controlled by the operator:

1. **Raft traffic is password-authenticated, not encrypted.** An attacker positioned on the cluster-internal network who captures Raft traffic learns cluster membership topology. Control: network segmentation (Juju spaces / security groups), and accepting that the Raft password does not protect confidentiality of consensus traffic.
2. **The watcher is a consensus participant.** A compromised watcher host can participate in — but not outvote — the Raft cluster (2 PostgreSQL votes vs 1 watcher vote); it can attempt disruption of quorum. Controls: host hardening, model access discipline, minimal exposure of port 2222.
3. **Losing the watcher returns the cluster to 2-member partition behaviour.** This is availability-equivalent to not having a watcher — it degrades the guarantee the watcher exists to provide, it does not endanger data. Controls: self-healing (service auto-restart), availability-zone separation, monitoring the unit status.
4. **Relation data poisoning** — a charm-side attacker able to write the PostgreSQL application's relation data could point the watcher at attacker-chosen endpoints. Juju application-scoped writes make this equivalent to compromising the PostgreSQL charm; no additional watcher-side control applies.

### Hardening benchmarks

The watcher is not certified against FIPS 140-3, CIS, or any other hardening benchmark. (Ubuntu Pro FIPS modules apply to the archive, not to this charm's packaging; do not claim FIPS compliance for a deployment because it contains a watcher.)

### Logging and monitoring

- The Raft controller service logs to the systemd journal (`StandardOutput=journal`); charm and hook logs go to the Juju agent log. View with `juju debug-log` and `journalctl -u watcher-raft@<relation-id>`.
- Unit status is the primary health signal: `Active` with a message of the form `Raft connected, monitoring N PostgreSQL endpoints`; Waiting/Blocked states report the reason (no relation, Raft not connected, AZ co-location in production, odd-member warning).
- There is no separate audit trail; administrative actions on the watcher are Juju operations and appear in the Juju controller audit log if enabled.
- No PII or secrets are written to logs <!-- TODO(SEC0030 review): confirm no password is written to logged config -->. COS integration for alerting follows the Charmed PostgreSQL monitoring setup; the watcher itself exports no metrics endpoint.

## Decommissioning the product securely

1. **Removing the relation** (`juju remove-relation`): the watcher automatically removes its Raft membership from the cluster, stops and disables the per-relation systemd service, and releases the allocated port. No manual cleanup is required for the cluster side.
2. **Removing the application** (`juju remove-application postgresql-watcher`): removes the unit and charm code. The `charmed-postgresql` snap remains installed; remove it explicitly if no other charm on the host uses it: `snap remove charmed-postgresql`.
3. **Data deletion**: removing the relation/application deletes the Raft data directories and port-allocation state under `/var/snap/charmed-postgresql/common/watcher-raft/` <!-- TODO(SEC0030 review): confirm the relation-broken handler removes the data directories -->; the systemd unit file `/etc/systemd/system/watcher-raft@.service` is disabled by the charm but only deleted on snap removal. There is no scheduled/automatic deletion beyond the above — verify the tree is gone if the host is being repurposed.
4. **User data export**: not applicable — the watcher stores no user data, ever.
5. **Credential disposal**: the Raft and watcher passwords are Juju secrets owned by the Charmed PostgreSQL application; removing the relation/application triggers secret revision removal. On the PostgreSQL side, drop the `watcher` user if the cluster remains.
6. **Notification of end-of-support**: the watcher follows the [Charmed PostgreSQL release and support lifecycle](charm-versions); end-of-support is announced through the Charmed PostgreSQL release notes.
7. **End-of-support impact**: after security maintenance ends, continued use means no security patches for the packaging; the statement in the Charmed PostgreSQL lifecycle documentation applies unchanged.
8. **Removal from the environment**: covered by (1)–(3).
9. **Third-party operated parts**: none — the watcher is fully contained in the Juju model that deploys it.

## Security lifecycle

- **Versions and support window**: the watcher tracks Charmed PostgreSQL 16; its `16/stable` and `16/edge` channels align with the Charmed PostgreSQL 16 lifecycle. Security-maintained channel: `16/stable`. <!-- TODO(SEC0030 review): confirm the watcher EOL commitment statement -->
- **How updates are delivered**: charm upgrades via `juju refresh`, which drives a coordinated rolling refresh (snap revision pinned per charm release; `pre-refresh-check` action validates readiness; `pause-after-unit-refresh` config gates progression; `force-refresh-start` / `resume-refresh` actions manage exceptions). Dependency updates land continuously on `16/edge` through automated dependency management and ride the next charm release.
- **Delaying updates**: deferring a `juju refresh` that carries security fixes leaves the known vulnerabilities in place for the delay period; High/Critical fixes are expected to be applied in the current or next immediate release (Canonical's vulnerability response standard).
- **Verifying an update**: after refresh, `juju status` shows the charm revision the unit is running; `snap list charmed-postgresql` shows the snap revision; the release notes for each charm revision list the dependency changes it carries.

## Reporting vulnerabilities and/or bugs

- Report security issues privately via the repository's security policy: [canonical/postgresql-watcher-operator/SECURITY.md](https://github.com/canonical/postgresql-watcher-operator/blob/16/edge/SECURITY.md) (GitHub private vulnerability reporting).
- Canonical's disclosure and embargo policy: [Ubuntu Security disclosure and embargo policy](https://ubuntu.com/security/disclosure-policy).
- Known vulnerabilities affecting published releases are recorded in the repository's GitHub security advisories and in the release notes of each charm revision.
- Non-security bugs: [repository issue tracker](https://github.com/canonical/postgresql-watcher-operator/issues).

## See also

* {ref}`explanation-stereo-mode` — how the watcher participates in a cluster, and how to deploy and integrate it.
* {ref}`security-hardening-overview` — environment-level hardening that applies equally to watcher hosts.
* {ref}`cryptography` — the cryptography mechanisms used by Charmed PostgreSQL.

