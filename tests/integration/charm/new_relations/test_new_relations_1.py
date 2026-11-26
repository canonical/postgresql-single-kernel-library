# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import logging
import time

import jubilant
import psycopg2
import pytest
from single_kernel_postgresql.config.enums import Substrates
from single_kernel_postgresql.config.literals import DATABASE_DEFAULT_NAME
from tenacity import Retrying, stop_after_attempt, stop_after_delay, wait_fixed

from tests.integration.charm.helpers import (
    CHARM_BASE,
    assert_sync_standbys,
    build_connection_string,
    check_database_users_existence,
    get_application_relation_data,
    get_machine_from_unit,
    get_patroni_cluster,
    get_primary_from_app,
    scale_ha_application,
    start_machine,
    stop_machine,
)
from tests.integration.helpers.juju import (
    fast_forward,
    get_app_leader,
    get_unit_address,
    split_model,
    wait_for_apps_status,
)

logger = logging.getLogger(__name__)

APPLICATION_APP_NAME = "postgresql-test-app"
DATABASE_APP_NAME = "database"
ANOTHER_DATABASE_APP_NAME = "another-database"
DATA_INTEGRATOR_APP_NAME = "data-integrator"
APP_NAMES = [APPLICATION_APP_NAME, DATABASE_APP_NAME, ANOTHER_DATABASE_APP_NAME]
FIRST_DATABASE_RELATION_NAME = "database"
SECOND_DATABASE_RELATION_NAME = "second-database"
MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME = "multiple-database-clusters"
ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME = "aliased-multiple-database-clusters"
NO_DATABASE_RELATION_NAME = "no-database"
INVALID_EXTRA_USER_ROLE_BLOCKING_MESSAGE = "invalid role(s) for extra user roles"


@pytest.mark.vm_only
@pytest.mark.abort_on_fail
def test_deploy_charms(juju: jubilant.Juju, charm):
    """Deploy both charms (application and database) to use in the tests."""
    # Deploy both charms (multiple units for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    with fast_forward(juju):
        juju.deploy(
            APPLICATION_APP_NAME,
            app=APPLICATION_APP_NAME,
            num_units=2,
            channel="latest/edge",
            base=CHARM_BASE,
        )
        juju.deploy(
            charm,
            app=DATABASE_APP_NAME,
            num_units=1,
            base=CHARM_BASE,
            config={"profile": "testing"},
        )
        juju.deploy(
            charm,
            app=ANOTHER_DATABASE_APP_NAME,
            num_units=2,
            base=CHARM_BASE,
            config={"profile": "testing"},
        )

        juju.wait(
            wait_for_apps_status(
                jubilant.all_active, DATABASE_APP_NAME, ANOTHER_DATABASE_APP_NAME
            ),
            timeout=3000,
        )


@pytest.mark.k8s_only
@pytest.mark.abort_on_fail
def test_database_relation_with_charm_libraries_k8s(
    juju: jubilant.Juju, charm, charm_resources, substrate: Substrates
):
    """Test basic functionality of database relation interface."""
    # Deploy both charms (multiple units for each application to test that later they correctly
    # set data in the relation application databag using only the leader unit).
    with fast_forward(juju):
        juju.deploy(
            APPLICATION_APP_NAME,
            app=APPLICATION_APP_NAME,
            num_units=2,
            base=CHARM_BASE,
            channel="latest/edge",
        )
        juju.deploy(
            charm,
            resources=charm_resources,
            app=DATABASE_APP_NAME,
            num_units=3,
            base=CHARM_BASE,
            trust=True,
            config={"profile": "testing"},
        )
        juju.deploy(
            charm,
            resources=charm_resources,
            app=ANOTHER_DATABASE_APP_NAME,
            num_units=3,
            base=CHARM_BASE,
            trust=True,
            config={"profile": "testing"},
        )
        # Relate the charms and wait for them exchanging some connection data.
        juju.integrate(f"{APPLICATION_APP_NAME}:{FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME)
        juju.wait(
            lambda status: (
                len(status.apps[DATABASE_APP_NAME].units) == 3
                and wait_for_apps_status(jubilant.all_active, DATABASE_APP_NAME)(status)
            ),
            timeout=1000,
            successes=31,
        )
        model_name = split_model(juju.model)[1]
        for attempt in Retrying(stop=stop_after_delay(600), wait=wait_fixed(3), reraise=True):
            with attempt:
                primary = get_primary_from_app(juju, DATABASE_APP_NAME)
                members = get_patroni_cluster(get_unit_address(juju, primary))["members"]
                assert (
                    len(members) == 3
                    and all(member["state"] in ("running", "streaming") for member in members)
                    and sum(member["role"] == "leader" for member in members) == 1
                ), f"Cluster not ready: {members}"
                assert (
                    get_application_relation_data(
                        juju,
                        APPLICATION_APP_NAME,
                        FIRST_DATABASE_RELATION_NAME,
                        "read-only-endpoints",
                    )
                    == f"{DATABASE_APP_NAME}-replicas.{model_name}.svc.cluster.local:5432"
                )

        # The pinned libjuju has list_secrets, so the original always checks this.
        logger.info("checking for secrets")
        secret_uri = get_application_relation_data(
            juju, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "secret-user"
        )
        password = get_application_relation_data(
            juju, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "password"
        )
        assert secret_uri is not None
        assert password is None

    # Get the connection string to connect to the database using the read/write endpoint.
    connection_string = build_connection_string(
        juju, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, substrate
    )

    # Connect to the database using the read/write endpoint.
    with psycopg2.connect(connection_string) as connection, connection.cursor() as cursor:
        # Check that it's possible to write and read data from the database that
        # was created for the application.
        connection.autocommit = True
        cursor.execute("DROP TABLE IF EXISTS test;")
        cursor.execute("CREATE TABLE test(data TEXT);")
        cursor.execute("INSERT INTO test(data) VALUES('some data');")
        cursor.execute("SELECT data FROM test;")
        data = cursor.fetchone()
        assert data[0] == "some data"

        # Check the version that the application received is the same on the database server.
        cursor.execute("SELECT version();")
        data = cursor.fetchone()[0].split(" ")[1]

        # Get the version of the database and compare with the information that
        # was retrieved directly from the database.
        version = get_application_relation_data(
            juju, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "version"
        )
        assert version == data

    # Get the connection string to connect to the database using the read-only endpoint.
    connection_string = build_connection_string(
        juju,
        APPLICATION_APP_NAME,
        FIRST_DATABASE_RELATION_NAME,
        substrate,
        read_only_endpoint=True,
    )

    # Connect to the database using the read-only endpoint.
    with psycopg2.connect(connection_string) as connection, connection.cursor() as cursor:
        # Read some data.
        cursor.execute("SELECT data FROM test;")
        data = cursor.fetchone()
        assert data[0] == "some data"

        # Try to alter some data in a read-only transaction.
        with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
            cursor.execute("DROP TABLE test;")


@pytest.mark.vm_only
def test_primary_read_only_endpoint_in_standalone_cluster_vm(juju: jubilant.Juju):
    """Test that there is no read-only endpoint in a standalone cluster."""
    with fast_forward(juju):
        # Ensure the cluster starts with only one member.
        # We can't scale down a running cluster to 1 unit because the way
        # Patroni raft implementation works (to scale from 2 units to 1 Patroni
        # needs at least one mode unit that run only raft to have quorum).
        assert len(juju.status().apps[DATABASE_APP_NAME].units) == 1

        # Relate the charms and wait for them exchanging some connection data.
        juju.integrate(f"{APPLICATION_APP_NAME}:{FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME)
        juju.wait(wait_for_apps_status(jubilant.all_active, *APP_NAMES), timeout=600)

        # Check that on juju 3 we have secrets and no username and password in the rel databag
        logger.info("checking for secrets")
        secret_uri = get_application_relation_data(
            juju, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "secret-user"
        )
        password = get_application_relation_data(
            juju, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "password"
        )
        assert secret_uri is not None
        assert password is None

        # Try to get the connection string of the database using the read-only endpoint.
        # It should be the primary.
        primary_unit = next(iter(juju.status().apps[DATABASE_APP_NAME].units.values()))
        for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(3), reraise=True):
            with attempt:
                data = get_application_relation_data(
                    juju,
                    APPLICATION_APP_NAME,
                    FIRST_DATABASE_RELATION_NAME,
                    "read-only-endpoints",
                )
                assert data == f"{primary_unit.public_address}:5432"


@pytest.mark.vm_only
def test_read_only_endpoint_in_scaled_up_cluster_vm(juju: jubilant.Juju):
    """Test that there is read-only endpoint in a scaled up cluster."""
    with fast_forward(juju):
        # Scale up the database.
        scale_ha_application(juju, DATABASE_APP_NAME, 2)
        primary = get_primary_from_app(juju, DATABASE_APP_NAME)
        replica = next(
            unit
            for name, unit in juju.status().apps[DATABASE_APP_NAME].units.items()
            if name != primary
        )

        # Try to get the connection string of the database using the read-only endpoint.
        # It should be the replica unit.
        for attempt in Retrying(stop=stop_after_delay(120), wait=wait_fixed(3), reraise=True):
            with attempt:
                data = get_application_relation_data(
                    juju,
                    APPLICATION_APP_NAME,
                    FIRST_DATABASE_RELATION_NAME,
                    "read-only-endpoints",
                )
                assert data == f"{replica.public_address}:5432"


@pytest.mark.vm_only
def test_database_relation_with_charm_libraries_vm(juju: jubilant.Juju, substrate: Substrates):
    """Test basic functionality of database relation interface."""
    # Get the connection string to connect to the database using the read/write endpoint.
    connection_string = build_connection_string(
        juju, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, substrate
    )

    # Connect to the database using the read/write endpoint.
    with psycopg2.connect(connection_string) as connection, connection.cursor() as cursor:
        # Check that it's possible to write and read data from the database that
        # was created for the application.
        connection.autocommit = True
        cursor.execute("DROP TABLE IF EXISTS test;")
        cursor.execute("CREATE TABLE test(data TEXT);")
        cursor.execute("INSERT INTO test(data) VALUES('some data');")
        cursor.execute("SELECT data FROM test;")
        data = cursor.fetchone()
        assert data[0] == "some data"

        # Check the version that the application received is the same on the database server.
        cursor.execute("SELECT version();")
        data = cursor.fetchone()[0].split(" ")[1]

        # Get the version of the database and compare with the information that
        # was retrieved directly from the database.
        version = get_application_relation_data(
            juju, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "version"
        )
        assert version == data

    # Get the connection string to connect to the database using the read-only endpoint.
    connection_string = build_connection_string(
        juju,
        APPLICATION_APP_NAME,
        FIRST_DATABASE_RELATION_NAME,
        substrate,
        read_only_endpoint=True,
    )

    # Connect to the database using the read-only endpoint.
    with psycopg2.connect(connection_string) as connection, connection.cursor() as cursor:
        # Read some data.
        cursor.execute("SELECT data FROM test;")
        data = cursor.fetchone()
        assert data[0] == "some data"

        # Try to alter some data in a read-only transaction.
        with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
            cursor.execute("DROP TABLE test;")


@pytest.mark.vm_only
@pytest.mark.abort_on_fail
def test_filter_out_degraded_replicas(juju: jubilant.Juju):
    primary = get_primary_from_app(juju, DATABASE_APP_NAME)
    replica = next(name for name in juju.status().apps[DATABASE_APP_NAME].units if name != primary)
    machine = get_machine_from_unit(juju, replica)
    stop_machine(machine)

    for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(3), reraise=True):
        # Topology observer runs every half a minute
        time.sleep(60)
        with attempt:
            data = get_application_relation_data(
                juju,
                APPLICATION_APP_NAME,
                FIRST_DATABASE_RELATION_NAME,
                "read-only-endpoints",
            )
            assert data == f"{get_unit_address(juju, primary)}:5432"

    start_machine(machine)
    juju.wait(wait_for_apps_status(jubilant.all_active, DATABASE_APP_NAME), timeout=200)


def test_two_applications_doesnt_share_the_same_relation_data(
    juju: jubilant.Juju, substrate: Substrates
):
    """Test that two different application connect to the database with different credentials."""
    # Set some variables to use in this test.
    another_application_app_name = "another-application"
    all_app_names = [another_application_app_name]
    all_app_names.extend(APP_NAMES)

    # Deploy another application.
    juju.deploy(
        APPLICATION_APP_NAME,
        app=another_application_app_name,
        base=CHARM_BASE,
        channel="latest/edge",
    )

    # Relate the new application with the database
    # and wait for them exchanging some connection data.
    juju.integrate(
        f"{another_application_app_name}:{FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME
    )
    juju.wait(wait_for_apps_status(jubilant.all_active, *all_app_names), timeout=600)

    # Assert the two application have different relation (connection) data.
    application_connection_string = build_connection_string(
        juju, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, substrate
    )
    another_application_connection_string = build_connection_string(
        juju, another_application_app_name, FIRST_DATABASE_RELATION_NAME, substrate
    )

    assert application_connection_string != another_application_connection_string

    # Check that the user cannot access other databases.
    for application, other_application_database in [
        (APPLICATION_APP_NAME, "another_application_database"),
        (another_application_app_name, f"{APPLICATION_APP_NAME.replace('-', '_')}_database"),
    ]:
        connection_string = build_connection_string(
            juju,
            application,
            FIRST_DATABASE_RELATION_NAME,
            substrate,
            database=DATABASE_DEFAULT_NAME,
        )
        with pytest.raises(psycopg2.Error):
            psycopg2.connect(connection_string)
        connection_string = build_connection_string(
            juju,
            application,
            FIRST_DATABASE_RELATION_NAME,
            substrate,
            database=other_application_database,
        )
        with pytest.raises(psycopg2.Error):
            psycopg2.connect(connection_string)


def test_an_application_can_connect_to_multiple_database_clusters(
    juju: jubilant.Juju, substrate: Substrates
):
    """Test that an application can connect to different clusters of the same database."""
    # Relate the application with both database clusters
    # and wait for them exchanging some connection data.
    juju.integrate(
        f"{APPLICATION_APP_NAME}:{MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}", DATABASE_APP_NAME
    )
    juju.integrate(
        f"{APPLICATION_APP_NAME}:{MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}",
        ANOTHER_DATABASE_APP_NAME,
    )
    juju.wait(wait_for_apps_status(jubilant.all_active, *APP_NAMES), timeout=600)

    # integrate returns no relation object: recover each ID by endpoint and related app.
    relation_data = juju.show_unit(f"{APPLICATION_APP_NAME}/0").relation_info
    first_cluster_relation = next(
        relation.relation_id
        for relation in relation_data
        if relation.endpoint == MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME
        and any(unit.split("/")[0] == DATABASE_APP_NAME for unit in relation.related_units)
    )
    second_cluster_relation = next(
        relation.relation_id
        for relation in relation_data
        if relation.endpoint == MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME
        and any(unit.split("/")[0] == ANOTHER_DATABASE_APP_NAME for unit in relation.related_units)
    )

    # Retrieve the connection string to both database clusters using the relation aliases
    # and assert they are different.
    application_connection_string = build_connection_string(
        juju,
        APPLICATION_APP_NAME,
        MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        substrate,
        relation_id=first_cluster_relation,
    )
    another_application_connection_string = build_connection_string(
        juju,
        APPLICATION_APP_NAME,
        MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
        substrate,
        relation_id=second_cluster_relation,
    )
    assert application_connection_string != another_application_connection_string


# @pytest.mark.skip(reason="Unstable")
# async def test_an_application_can_connect_to_multiple_aliased_database_clusters(
#     ops_test: OpsTest,
# ):
#     """Test that an application can connect to different clusters of the same database."""
#     # Relate the application with both database clusters
#     # and wait for them exchanging some connection data.
#     await asyncio.gather(
#         ops_test.model.add_relation(
#             f"{APPLICATION_APP_NAME}:{ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}",
#             DATABASE_APP_NAME,
#         ),
#         ops_test.model.add_relation(
#             f"{APPLICATION_APP_NAME}:{ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME}",
#             ANOTHER_DATABASE_APP_NAME,
#         ),
#     )
#     await ops_test.model.wait_for_idle(apps=APP_NAMES, status="active")
#
#     # Retrieve the connection string to both database clusters using the relation aliases
#     # and assert they are different.
#     for attempt in Retrying(stop=stop_after_attempt(5), wait=wait_fixed(3), reraise=True):
#         with attempt:
#             application_connection_string = await build_connection_string(
#                 ops_test,
#                 APPLICATION_APP_NAME,
#                 ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
#                 relation_alias="cluster1",
#             )
#             another_application_connection_string = await build_connection_string(
#                 ops_test,
#                 APPLICATION_APP_NAME,
#                 ALIASED_MULTIPLE_DATABASE_CLUSTERS_RELATION_NAME,
#                 relation_alias="cluster2",
#             )
#     assert application_connection_string != another_application_connection_string


@pytest.mark.abort_on_fail
def test_an_application_can_request_multiple_databases(juju: jubilant.Juju, substrate: Substrates):
    """Test that an application can request additional databases using the same interface."""
    # Relate the charms using another relation and wait for them exchanging some connection data.
    juju.integrate(f"{APPLICATION_APP_NAME}:{SECOND_DATABASE_RELATION_NAME}", DATABASE_APP_NAME)
    juju.wait(wait_for_apps_status(jubilant.all_active, *APP_NAMES), timeout=15 * 60)

    # Get the connection strings to connect to both databases.
    for attempt in Retrying(stop=stop_after_attempt(15), wait=wait_fixed(3), reraise=True):
        with attempt:
            first_database_connection_string = build_connection_string(
                juju, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, substrate
            )
            second_database_connection_string = build_connection_string(
                juju, APPLICATION_APP_NAME, SECOND_DATABASE_RELATION_NAME, substrate
            )

    # Assert the two application have different relation (connection) data.
    assert first_database_connection_string != second_database_connection_string


@pytest.mark.k8s_only
def test_primary_read_only_endpoint_in_standalone_cluster_k8s(juju: jubilant.Juju):
    """Test that there is no read-only endpoint in a standalone cluster."""
    with fast_forward(juju):
        # Scale down the database.
        scale_ha_application(juju, DATABASE_APP_NAME, 1)

        # Try to get the connection string of the database using the read-only endpoint.
        # It should not be available anymore.
        for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(3), reraise=True):
            with attempt:
                data = get_application_relation_data(
                    juju,
                    APPLICATION_APP_NAME,
                    FIRST_DATABASE_RELATION_NAME,
                    "read-only-endpoints",
                )
                model_name = split_model(juju.model)[1]
                assert data == f"{DATABASE_APP_NAME}-primary.{model_name}.svc.cluster.local:5432"


@pytest.mark.k8s_only
def test_read_only_endpoint_in_scaled_up_cluster_k8s(juju: jubilant.Juju):
    """Test that there is read-only endpoint in a scaled up cluster."""
    with fast_forward(juju):
        # Scale up the database.
        scale_ha_application(juju, DATABASE_APP_NAME, 3)

        # Try to get the connection string of the database using the read-only endpoint.
        # It should be available again.
        for attempt in Retrying(stop=stop_after_attempt(3), wait=wait_fixed(3), reraise=True):
            with attempt:
                data = get_application_relation_data(
                    juju,
                    APPLICATION_APP_NAME,
                    FIRST_DATABASE_RELATION_NAME,
                    "read-only-endpoints",
                )
                model_name = split_model(juju.model)[1]
                assert data == f"{DATABASE_APP_NAME}-replicas.{model_name}.svc.cluster.local:5432"


@pytest.mark.k8s_only
def test_relation_broken(juju: jubilant.Juju):
    """Test that the user is removed when the relation is broken."""
    with fast_forward(juju):
        # Retrieve the relation user.
        relation_user = get_application_relation_data(
            juju, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, "username"
        )

        # Break the relation.
        juju.remove_relation(
            f"{DATABASE_APP_NAME}", f"{APPLICATION_APP_NAME}:{FIRST_DATABASE_RELATION_NAME}"
        )
        juju.wait(wait_for_apps_status(jubilant.all_active, *APP_NAMES), timeout=600)

        # Check that the relation user was removed from the database.
        check_database_users_existence(
            juju, [], [relation_user], database_app_name=DATABASE_APP_NAME
        )


@pytest.mark.k8s_only
def test_restablish_relation(juju: jubilant.Juju, substrate: Substrates):
    """Test that a previously broken relation would be functional if restored."""
    # Relate the charms and wait for them exchanging some connection data.
    with fast_forward(juju):
        juju.integrate(f"{APPLICATION_APP_NAME}:{FIRST_DATABASE_RELATION_NAME}", DATABASE_APP_NAME)
        juju.wait(wait_for_apps_status(jubilant.all_active, *APP_NAMES), timeout=600)

        # Workload statuses can remain active while the new relation data propagates.
        for attempt in Retrying(stop=stop_after_delay(600), wait=wait_fixed(3), reraise=True):
            with attempt:
                for key in ("endpoints", "read-only-endpoints"):
                    assert get_application_relation_data(
                        juju, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, key
                    ), f"Restored relation has no {key} yet"

    # Get the connection string to connect to the database using the read-only endpoint.
    connection_string = build_connection_string(
        juju,
        APPLICATION_APP_NAME,
        FIRST_DATABASE_RELATION_NAME,
        substrate,
        read_only_endpoint=True,
    )

    # Connect to the database using the read-only endpoint.
    with psycopg2.connect(connection_string) as connection, connection.cursor() as cursor:
        # Check that preexisting data is still accessible.
        cursor.execute("SELECT data FROM test;")
        data = cursor.fetchone()
        assert data[0] == "some data"

    # Get the connection string to connect to the database using the read/write endpoint.
    connection_string = build_connection_string(
        juju, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, substrate
    )

    # Connect to the database using the read/write endpoint.
    with psycopg2.connect(connection_string) as connection, connection.cursor() as cursor:
        # Check that it's possible to write and read data from the database.
        connection.autocommit = True
        cursor.execute("DELETE FROM test;")
        cursor.execute("INSERT INTO test(data) VALUES('other data');")
        cursor.execute("SELECT data FROM test;")
        data = cursor.fetchone()
        assert data[0] == "other data"


@pytest.mark.vm_only
@pytest.mark.abort_on_fail
def test_relation_data_is_updated_correctly_when_scaling(
    juju: jubilant.Juju, substrate: Substrates
):
    """Test that relation data, like connection data, is updated correctly when scaling."""
    # Retrieve the list of current database unit names.
    units_to_remove = list(juju.status().apps[DATABASE_APP_NAME].units)

    with fast_forward(juju, interval="60s"):
        # Add two more units.
        juju.add_unit(DATABASE_APP_NAME, num_units=2)
        juju.wait(wait_for_apps_status(jubilant.all_active, DATABASE_APP_NAME), timeout=3000)

        first_unit = next(iter(juju.status().apps[DATABASE_APP_NAME].units.values()))
        assert_sync_standbys(first_unit.public_address, 2)

        # Remove the original units.
        leader_unit = get_app_leader(juju, DATABASE_APP_NAME)
        juju.remove_unit(*[unit for unit in units_to_remove if unit != leader_unit])
        juju.wait(wait_for_apps_status(jubilant.all_active, DATABASE_APP_NAME), timeout=600)
        juju.remove_unit(leader_unit)
        juju.wait(wait_for_apps_status(jubilant.all_active, DATABASE_APP_NAME), timeout=600)

        # Get the updated connection data and assert it can be used
        # to write and read some data properly.
        primary_connection_string = build_connection_string(
            juju, APPLICATION_APP_NAME, FIRST_DATABASE_RELATION_NAME, substrate
        )
        replica_connection_string = build_connection_string(
            juju,
            APPLICATION_APP_NAME,
            FIRST_DATABASE_RELATION_NAME,
            substrate,
            read_only_endpoint=True,
        )

        # Connect to the database using the primary connection string.
        with psycopg2.connect(primary_connection_string) as connection:
            connection.autocommit = True
            with connection.cursor() as cursor:
                # Check that it's possible to write and read data from the database that
                # was created for the application.
                cursor.execute("DROP TABLE IF EXISTS test;")
                cursor.execute("CREATE TABLE test(data TEXT);")
                cursor.execute("INSERT INTO test(data) VALUES('some data');")
                cursor.execute("SELECT data FROM test;")
                data = cursor.fetchone()
                assert data[0] == "some data"
        connection.close()

        # Connect to the database using the replica endpoint.
        with (
            psycopg2.connect(replica_connection_string) as connection,
            connection.cursor() as cursor,
        ):
            # Read some data.
            cursor.execute("SELECT data FROM test;")
            data = cursor.fetchone()
            assert data[0] == "some data"

            # Try to alter some data in a read-only transaction.
            with pytest.raises(psycopg2.errors.ReadOnlySqlTransaction):
                cursor.execute("DROP TABLE test;")
        connection.close()

    with fast_forward(juju):
        # Remove the relation and test that its user was deleted
        # (by checking that the connection string doesn't work anymore).
        juju.remove_relation(
            f"{DATABASE_APP_NAME}:database",
            f"{APPLICATION_APP_NAME}:{FIRST_DATABASE_RELATION_NAME}",
        )
        juju.wait(wait_for_apps_status(jubilant.all_active, DATABASE_APP_NAME), timeout=1000)
        with pytest.raises(psycopg2.OperationalError):
            psycopg2.connect(primary_connection_string)


@pytest.mark.abort_on_fail
def test_relation_with_no_database_name(juju: jubilant.Juju):
    """Test that a relation with no database name doesn't block the charm."""
    with fast_forward(juju):
        # Relate the charms using a relation that doesn't provide a database name.
        juju.integrate(f"{APPLICATION_APP_NAME}:{NO_DATABASE_RELATION_NAME}", DATABASE_APP_NAME)
        juju.wait(wait_for_apps_status(jubilant.all_active, *APP_NAMES), timeout=600)

        # Break the relation.
        juju.remove_relation(
            f"{DATABASE_APP_NAME}", f"{APPLICATION_APP_NAME}:{NO_DATABASE_RELATION_NAME}"
        )
        juju.wait(wait_for_apps_status(jubilant.all_active, *APP_NAMES), timeout=600)


@pytest.mark.abort_on_fail
def test_invalid_extra_user_roles(juju: jubilant.Juju):
    with fast_forward(juju):
        juju.deploy(DATA_INTEGRATOR_APP_NAME, channel="latest/edge", base=CHARM_BASE)
        juju.wait(
            wait_for_apps_status(jubilant.all_blocked, DATA_INTEGRATOR_APP_NAME), timeout=600
        )

        another_data_integrator_app_name = f"another-{DATA_INTEGRATOR_APP_NAME}"
        data_integrator_apps_names = [DATA_INTEGRATOR_APP_NAME, another_data_integrator_app_name]
        juju.deploy(
            DATA_INTEGRATOR_APP_NAME,
            app=another_data_integrator_app_name,
            channel="latest/edge",
            base=CHARM_BASE,
        )
        juju.wait(
            wait_for_apps_status(jubilant.all_blocked, another_data_integrator_app_name),
            timeout=600,
        )
        for app in data_integrator_apps_names:
            juju.config(
                app,
                {
                    "database-name": app.replace("-", "_"),
                    "extra-user-roles": "test",
                },
            )
        juju.wait(
            wait_for_apps_status(jubilant.all_blocked, *data_integrator_apps_names), timeout=600
        )
        for app in data_integrator_apps_names:
            juju.integrate(f"{app}:postgresql", f"{DATABASE_APP_NAME}:database")
        juju.wait(lambda status: jubilant.all_agents_idle(status, DATABASE_APP_NAME), timeout=600)
        juju.wait(
            lambda status: any(
                unit.workload_status.message == INVALID_EXTRA_USER_ROLE_BLOCKING_MESSAGE
                for unit in status.apps[DATABASE_APP_NAME].units.values()
            ),
            timeout=1000,
            delay=0.5,
            successes=1,
        )

        # Verify that the charm remains blocked if there are still other relations with invalid
        # extra user roles.
        juju.remove_relation(
            f"{DATABASE_APP_NAME}:database", f"{DATA_INTEGRATOR_APP_NAME}:postgresql"
        )
        juju.wait(lambda status: jubilant.all_agents_idle(status, DATABASE_APP_NAME), timeout=600)
        juju.wait(
            lambda status: any(
                unit.workload_status.message == INVALID_EXTRA_USER_ROLE_BLOCKING_MESSAGE
                for unit in status.apps[DATABASE_APP_NAME].units.values()
            ),
            timeout=1000,
            delay=0.5,
            successes=1,
        )

        # Verify that active status is restored after all relations are removed.
        juju.remove_relation(
            f"{DATABASE_APP_NAME}:database", f"{another_data_integrator_app_name}:postgresql"
        )
        juju.wait(wait_for_apps_status(jubilant.all_active, DATABASE_APP_NAME), timeout=1000)
