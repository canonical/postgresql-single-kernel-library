(contribute)=
# Contribute

Contributions to Charmed PostgreSQL operators are welcomed and encouraged.

If you'd like to contribute but aren't sure how to start,{ref}`get in touch with us <contact>` first so we can help you navigate your contribution process.

## Development

See the contributing guides in

* `postgresql-operator` (machine charm) on the `14/edge` branch: [14/edge/CONTRIBUTING.md](https://github.com/canonical/postgresql-operator/blob/14/edge/CONTRIBUTING.md)
* `postgresql-k8s-operator` (K8s charm) on the `14/edge` branch: [14/edge/CONTRIBUTING.md](https://github.com/canonical/postgresql-k8s-operator/blob/14/edge/CONTRIBUTING.md)

## Documentation

Contributions to Charmed PostgreSQL documentation are welcomed and encouraged.

```{dropdown} All documentation is in the <code>postgresql-single-kernel-library</code> repository
:open:
:class-container: dropdown-note
:icon: info

Although the code for PostgreSQL 14 operators remains in the separate `postgresql-operator` and `postgresql-k8s-operator` repositories, the documentation for **all Charmed PostgreSQL operators and versions** has transferred to the [`postgresql-single-kernel-library` repository](https://github.com/canonical/postgresql-single-kernel-library):

* `postgresql-single-kernel-library` branch [`16/docs`](https://github.com/canonical/postgresql-single-kernel-library/tree/16/docs) contains the docs for both VM and K8s charms for PostgreSQL 16 (newest)
* `postgresql-single-kernel-library` branch [`14/docs`](https://github.com/canonical/postgresql-single-kernel-library/tree/14/docs) contains the docs for both VM and K8s charms for PostgreSQL 14
```

### Prerequisites

 * A GitHub account: See [Creating an account on GitHub](https://docs.github.com/en/get-started/start-your-journey/creating-an-account-on-github)
 * Code of Conduct compliance: You must read and follow the [Ubuntu Code of Conduct](https://ubuntu.com/community/ethos/code-of-conduct). Maintainers reserve the right to delete or remove any contributions that do not respect the Code of Conduct.

### Report an issue

To report an issue in spelling, grammar, or technical content, file an issue on GitHub with the `documentation` label. A maintainer will update it as soon as possible.

* `postgresql-operator` (machine charm): [File an issue](https://github.com/canonical/postgresql-operator/issues)
* `postgresql-k8s-operator` (K8s charm): [File an issue](https://github.com/canonical/postgresql-k8s-operator/issues)

### Make a contribution

To make a quick update yourself, the easiest way is to click the pencil icon in the top-right corner of the documentation page (next to the "Give feedback" button). It will take you to the GitHub web editor for that page, and you can submit the PR directly through the web interface.

```{dropdown} Documentation branch
:open:
:class-container: dropdown-caution
:icon: alert-fill

All documentation pull requests should be made against the `14/docs` branch.
```

For larger, more involved contributions, please create an issue on GitHub with the `documentation` tag, and assign it to yourself. A maintainer will get in touch with you. Alternatively, you are welcome to contact us directly via [Matrix](https://matrix.to/#/#charmhub-data-platform:ubuntu.com).

{{seealso}} {ref}`contact`
