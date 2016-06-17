.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

.. highlight:: none

Operating Cassandra
===================

Replication Strategies
----------------------

.. todo:: todo

Snitch
------

In cassandra, the snitch has two functions:

- it teaches Cassandra enough about your network topology to route requests efficiently.
- it allows Cassandra to spread replicas around your cluster to avoid correlated failures. It does this by grouping
  machines into "datacenters" and "racks."  Cassandra will do its best not to have more than one replica on the same
  "rack" (which may not actually be a physical location).

Dynamic snitching
^^^^^^^^^^^^^^^^^

The dynamic snitch monitor read latencies to avoid reading from hosts that have slowed down. The dynamic snitch is
configured with the following properties on ``cassandra.yaml``:

- ``dynamic_snitch``: whether the dynamic snitch should be enabled or disabled.
- ``dynamic_snitch_update_interval_in_ms``: controls how often to perform the more expensive part of host score
  calculation.
- ``dynamic_snitch_reset_interval_in_ms``: if set greater than zero and read_repair_chance is < 1.0, this will allow
  'pinning' of replicas to hosts in order to increase cache capacity.
- ``dynamic_snitch_badness_threshold:``: The badness threshold will control how much worse the pinned host has to be
  before the dynamic snitch will prefer other replicas over it.  This is expressed as a double which represents a
  percentage.  Thus, a value of 0.2 means Cassandra would continue to prefer the static snitch values until the pinned
  host was 20% worse than the fastest.

Snitch classes
^^^^^^^^^^^^^^

The ``endpoint_snitch`` parameter in ``cassandra.yaml`` should be set to the class the class that implements
``IEndPointSnitch`` which will be wrapped by the dynamic snitch and decide if two endpoints are in the same data center
or on the same rack. Out of the box, Cassandra provides the snitch implementations:

GossipingPropertyFileSnitch
    This should be your go-to snitch for production use. The rack and datacenter for the local node are defined in
    cassandra-rackdc.properties and propagated to other nodes via gossip. If ``cassandra-topology.properties`` exists,
    it is used as a fallback, allowing migration from the PropertyFileSnitch.

SimpleSnitch
    Treats Strategy order as proximity. This can improve cache locality when disabling read repair. Only appropriate for
    single-datacenter deployments.

PropertyFileSnitch
    Proximity is determined by rack and data center, which are explicitly configured in
    ``cassandra-topology.properties``.

Ec2Snitch
    Appropriate for EC2 deployments in a single Region. Loads Region and Availability Zone information from the EC2 API.
    The Region is treated as the datacenter, and the Availability Zone as the rack. Only private IPs are used, so this
    will not work across multiple regions.

Ec2MultiRegionSnitch
    Uses public IPs as broadcast_address to allow cross-region connectivity (thus, you should set seed addresses to the
    public IP as well). You will need to open the ``storage_port`` or ``ssl_storage_port`` on the public IP firewall
    (For intra-Region traffic, Cassandra will switch to the private IP after establishing a connection).

RackInferringSnitch
    Proximity is determined by rack and data center, which are assumed to correspond to the 3rd and 2nd octet of each
    node's IP address, respectively.  Unless this happens to match your deployment conventions, this is best used as an
    example of writing a custom Snitch class and is provided in that spirit.

Adding, replacing, moving and removing nodes
--------------------------------------------

Bootstrap
^^^^^^^^^

Adding new nodes is called "bootstrapping". The ``num_tokens`` parameter will define the amount of virtual nodes
(tokens) the joining node will be assigned during bootstrap. The tokens define the sections of the ring (token ranges)
the node will become responsible for.

Token allocation
~~~~~~~~~~~~~~~~

With the default token allocation algorithm the new node will pick ``num_tokens`` random tokens to become responsible
for. Since tokens are distributed randomly, load distribution improves with a higher amount of virtual nodes, but it
also increases token management overhead. The default of 256 virtual nodes should provide a reasonable load balance with
acceptable overhead.

On 3.0+ a new token allocation algorithm was introduced to allocate tokens based on the load of existing virtual nodes
for a given keyspace, and thus yield an improved load distribution with a lower number of tokens. To use this approach,
the new node must be started with the JVM option ``-Dcassandra.allocate_tokens_for_keyspace=<keyspace>``, where
``<keyspace>`` is the keyspace from which the algorithm can find the load information to optimize token assignment for.

Manual token assignment
"""""""""""""""""""""""

You may specify a comma-separated list of tokens manually with the ``initial_token`` ``cassandra.yaml`` parameter, and
if that is specified Cassandra will skip the token allocation process. This may be useful when doing token assignment
with an external tool or when restoring a node with its previous tokens.

Range streaming
~~~~~~~~~~~~~~~~

After the tokens are allocated, the joining node will pick current replicas of the token ranges it will become
responsible for to stream data from. By default it will stream from the primary replica of each token range in order to
guarantee data in the new node will be consistent with the current state.

In the case of any unavailable replica, the consistent bootstrap process will fail. To override this behavior and
potentially miss data from an unavailable replica, set the JVM flag ``-Dcassandra.consistent.rangemovement=false``.

Resuming failed/hanged bootstrap
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On 2.2+, if the bootstrap process fails, it's possible to resume bootstrap from the previous saved state by calling
``nodetool bootstrap resume``. If for some reason the bootstrap hangs or stalls, it may also be resumed by simply
restarting the node. In order to cleanup bootstrap state and start fresh, you may set the JVM startup flag
``-Dcassandra.reset_bootstrap_progress=true``.

On lower versions, when the bootstrap proces fails it is recommended to wipe the node (remove all the data), and restart
the bootstrap process again.

Manual bootstrapping
~~~~~~~~~~~~~~~~~~~~

It's possible to skip the bootstrapping process entirely and join the ring straight away by setting the hidden parameter
``auto_bootstrap: false``. This may be useful when restoring a node from a backup or creating a new data-center.

Removing nodes
^^^^^^^^^^^^^^

You can take a node out of the cluster with ``nodetool decommission`` to a live node, or ``nodetool removenode`` (to any
other machine) to remove a dead one. This will assign the ranges the old node was responsible for to other nodes, and
replicate the appropriate data there. If decommission is used, the data will stream from the decommissioned node. If
removenode is used, the data will stream from the remaining replicas.

No data is removed automatically from the node being decommissioned, so if you want to put the node back into service at
a different token on the ring, it should be removed manually.

Moving nodes
^^^^^^^^^^^^

When ``num_tokens: 1`` it's possible to move the node position in the ring with ``nodetool move``. Moving is both a
convenience over and more efficient than decommission + bootstrap. After moving a node, ``nodetool cleanup`` should be
run to remove any unnecessary data.

Replacing a dead node
^^^^^^^^^^^^^^^^^^^^^

In order to replace a dead node, start cassandra with the JVM startup flag
``-Dcassandra.replace_address_first_boot=<dead_node_ip>``. Once this property is enabled the node starts in a hibernate
state, during which all the other nodes will see this node to be down.

The replacing node will now start to bootstrap the data from the rest of the nodes in the cluster. The main difference
between normal bootstrapping of a new node is that this new node will not accept any writes during this phase.

Once the bootstrapping is complete the node will be marked "UP", we rely on the hinted handoff's for making this node
consistent (since we don't accept writes since the start of the bootstrap).

.. Note:: If the replacement process takes longer than ``max_hint_window_in_ms`` you **MUST** run repair to make the
   replaced node consistent again, since it missed ongoing writes during bootstrapping.

Monitoring progress
^^^^^^^^^^^^^^^^^^^

Bootstrap, replace, move and remove progress can be monitored using ``nodetool netstats`` which will show the progress
of the streaming operations.

Cleanup data after range movements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As a safety measure, Cassandra does not automatically remove data from nodes that "lose" part of their token range due
to a range movement operation (bootstrap, move, replace). Run ``nodetool cleanup`` on the nodes that lost ranges to the
joining node when you are satisfied the new node is up and working. If you do not do this the old data will still be
counted against the load on that node.

Repair
------

.. todo:: todo

Read repair
-----------

.. todo:: todo

Hints
-----

.. todo:: todo

Compaction
----------

Size Tiered
^^^^^^^^^^^

.. todo:: todo

Leveled
^^^^^^^

.. todo:: todo

TimeWindow
^^^^^^^^^^
.. todo:: todo

DateTiered
^^^^^^^^^^
.. todo:: todo

Tombstones and Garbage Collection (GC) Grace
--------------------------------------------

Why Tombstones
^^^^^^^^^^^^^^

When a delete request is received by Cassandra it does not actually remove the data from the underlying store. Instead
it writes a special piece of data known as a tombstone. The Tombstone represents the delete and causes all values which
occurred before the tombstone to not appear in queries to the database. This approach is used instead of removing values
because of the distributed nature of Cassandra.

Deletes without tombstones
~~~~~~~~~~~~~~~~~~~~~~~~~~

Imagine a three node cluster which has the value [A] replicated to every node.::

    [A], [A], [A]

If one of the nodes fails and and our delete operation only removes existing values we can end up with a cluster that
looks like::

    [], [], [A]

Then a repair operation would replace the value of [A] back onto the two
nodes which are missing the value.::

    [A], [A], [A]

This would cause our data to be resurrected even though it had been
deleted.

Deletes with Tombstones
~~~~~~~~~~~~~~~~~~~~~~~

Starting again with a three node cluster which has the value [A] replicated to every node.::

    [A], [A], [A]

If instead of removing data we add a tombstone record, our single node failure situation will look like this.::

    [A, Tombstone[A]], [A, Tombstone[A]], [A]

Now when we issue a repair the Tombstone will be copied to the replica, rather than the deleted data being
resurrected.::

    [A, Tombstone[A]], [A, Tombstone[A]], [A, Tombstone[A]]

Our repair operation will correctly put the state of the system to what we expect with the record [A] marked as deleted
on all nodes. This does mean we will end up accruing Tombstones which will permanently accumulate disk space. To avoid
keeping tombstones forever we have a parameter known as ``gc_grace_seconds`` for every table in Cassandra.

The gc_grace_seconds parameter and Tombstone Removal
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The table level ``gc_grace_seconds`` parameter controls how long Cassandra will retain tombstones through compaction
events before finally removing them. This duration should directly reflect the amount of time a user expects to allow
before recovering a failed node. After ``gc_grace_seconds`` has expired the tombstone can be removed meaning there will
no longer be any record that a certain piece of data was deleted. This means if a node remains down or disconnected for
longer than ``gc_grace_seconds`` it's deleted data will be repaired back to the other nodes and re-appear in the
cluster. This is basically the same as in the "Deletes without Tombstones" section. Note that tombstones will not be
removed until a compaction event even if ``gc_grace_seconds`` has elapsed.

The default value for ``gc_grace_seconds`` is 864000 which is equivalent to 10 days. This can be set when creating or
altering a table using ``WITH gc_grace_seconds``.


Bloom Filters
-------------

In the read path, Cassandra merges data on disk (in SSTables) with data in RAM (in memtables). To avoid checking every
SSTable data file for the partition being requested, Cassandra employs a data structure known as a bloom filter.

Bloom filters are a probabilistic data structure that allows Cassandra to determine one of two possible states: - The
data definitely does not exist in the given file, or - The data probably exists in the given file.

While bloom filters can not guarantee that the data exists in a given SSTable, bloom filters can be made more accurate
by allowing them to consume more RAM. Operators have the opportunity to tune this behavior per table by adjusting the
the ``bloom_filter_fp_chance`` to a float between 0 and 1.

The default value for ``bloom_filter_fp_chance`` is 0.1 for tables using LeveledCompactionStrategy and 0.01 for all
other cases.

Bloom filters are stored in RAM, but are stored offheap, so operators should not consider bloom filters when selecting
the maximum heap size.  As accuracy improves (as the ``bloom_filter_fp_chance`` gets closer to 0), memory usage
increases non-linearly - the bloom filter for ``bloom_filter_fp_chance = 0.01`` will require about three times as much
memory as the same table with ``bloom_filter_fp_chance = 0.1``.

Typical values for ``bloom_filter_fp_chance`` are usually between 0.01 (1%) to 0.1 (10%) false-positive chance, where
Cassandra may scan an SSTable for a row, only to find that it does not exist on the disk. The parameter should be tuned
by use case:

- Users with more RAM and slower disks may benefit from setting the ``bloom_filter_fp_chance`` to a numerically lower
  number (such as 0.01) to avoid excess IO operations
- Users with less RAM, more dense nodes, or very fast disks may tolerate a higher ``bloom_filter_fp_chance`` in order to
  save RAM at the expense of excess IO operations
- In workloads that rarely read, or that only perform reads by scanning the entire data set (such as analytics
  workloads), setting the ``bloom_filter_fp_chance`` to a much higher number is acceptable.

Changing
^^^^^^^^

The bloom filter false positive chance is visible in the ``DESCRIBE TABLE`` output as the field
``bloom_filter_fp_chance``. Operators can change the value with an ``ALTER TABLE`` statement:
::

    ALTER TABLE keyspace.table WITH bloom_filter_fp_chance=0.01

Operators should be aware, however, that this change is not immediate: the bloom filter is calculated when the file is
written, and persisted on disk as the Filter component of the SSTable. Upon issuing an ``ALTER TABLE`` statement, new
files on disk will be written with the new ``bloom_filter_fp_chance``, but existing sstables will not be modified until
they are compacted - if an operator needs a change to ``bloom_filter_fp_chance`` to take effect, they can trigger an
SSTable rewrite using ``nodetool scrub`` or ``nodetool upgradesstables -a``, both of which will rebuild the sstables on
disk, regenerating the bloom filters in the progress.


Compression
-----------

Cassandra offers operators the ability to configure compression on a per-table basis. Compression reduces the size of
data on disk by compressing the SSTable in user-configurable compression ``chunk_length_in_kb``. Because Cassandra
SSTables are immutable, the CPU cost of compressing is only necessary when the SSTable is written - subsequent updates
to data will land in different SSTables, so Cassandra will not need to decompress, overwrite, and recompress data when
UPDATE commands are issued. On reads, Cassandra will locate the relevant compressed chunks on disk, decompress the full
chunk, and then proceed with the remainder of the read path (merging data from disks and memtables, read repair, and so
on).

Configuring Compression
^^^^^^^^^^^^^^^^^^^^^^^

Compression is configured on a per-table basis as an optional argument to ``CREATE TABLE`` or ``ALTER TABLE``. By
default, three options are relevant:

- ``class`` specifies the compression class - Cassandra provides three classes (``LZ4Compressor``,
  ``SnappyCompressor``, and ``DeflateCompressor`` ). The default is ``SnappyCompressor``.
- ``chunk_length_in_kb`` specifies the number of kilobytes of data per compression chunk. The default is 64KB.
- ``crc_check_chance`` determines how likely Cassandra is to verify the checksum on each compression chunk during
  reads. The default is 1.0.

Users can set compression using the following syntax:

::

    CREATE TABLE keyspace.table (id int PRIMARY KEY) WITH compression = {'class': 'LZ4Compressor'};

Or

::

    ALTER TABLE keyspace.table WITH compression = {'class': 'SnappyCompressor', 'chunk_length_in_kb': 128, 'crc_check_chance': 0.5};

Once enabled, compression can be disabled with ``ALTER TABLE`` setting ``enabled`` to ``false``:

::

    ALTER TABLE keyspace.table WITH compression = {'enabled':'false'};

Operators should be aware, however, that changing compression is not immediate. The data is compressed when the SSTable
is written, and as SSTables are immutable, the compression will not be modified until the table is compacted. Upon
issuing a change to the compression options via ``ALTER TABLE``, the existing SSTables will not be modified until they
are compacted - if an operator needs compression changes to take effect immediately, the operator can trigger an SSTable
rewrite using ``nodetool scrub`` or ``nodetool upgradesstables -a``, both of which will rebuild the SSTables on disk,
re-compressing the data in the process.

Benefits and Uses
^^^^^^^^^^^^^^^^^

Compression's primary benefit is that it reduces the amount of data written to disk. Not only does the reduced size save
in storage requirements, it often increases read and write throughput, as the CPU overhead of compressing data is faster
than the time it would take to read or write the larger volume of uncompressed data from disk.

Compression is most useful in tables comprised of many rows, where the rows are similar in nature. Tables containing
similar text columns (such as repeated JSON blobs) often compress very well.

Operational Impact
^^^^^^^^^^^^^^^^^^

- Compression metadata is stored offheap and scales with data on disk.  This often requires 1-3GB of offheap RAM per
  terabyte of data on disk, though the exact usage varies with ``chunk_length_in_kb`` and compression ratios.

- Streaming operations involve compressing and decompressing data on compressed tables - in some code paths (such as
  non-vnode bootstrap), the CPU overhead of compression can be a limiting factor.

- The compression path checksums data to ensure correctness - while the traditional Cassandra read path does not have a
  way to ensure correctness of data on disk, compressed tables allow the user to set ``crc_check_chance`` (a float from
  0.0 to 1.0) to allow Cassandra to probabilistically validate chunks on read to verify bits on disk are not corrupt.

Advanced Use
^^^^^^^^^^^^

Advanced users can provide their own compression class by implementing the interface at
``org.apache.cassandra.io.compress.ICompressor``.

Backups
-------

.. todo:: todo

Monitoring
----------

JMX
^^^
.. todo:: todo

Metric Reporters
^^^^^^^^^^^^^^^^
.. todo:: todo

Security
--------

There are three main components to the security features provided by Cassandra:

- TLS/SSL encryption for client and inter-node communication
- Client authentication
- Authorization

TLS/SSL Encryption
^^^^^^^^^^^^^^^^^^

Cassandra provides secure communication between a client machine and a database cluster and between nodes within a
cluster. Enabling encryption ensures that data in flight is not compromised and is transferred securely. The options for
client-to-node and node-to-node encryption are managed separately and may be configured independently.

In both cases, the JVM defaults for supported protocols and cipher suites are used when encryption is enabled. These can
be overidden using the settings in ``cassandra.yaml``, but this is not recommended unless there are policies in place
which dictate certain settings or a need to disable vulnerable ciphers or protocols in cases where the JVM cannot be
updated.

FIPS compliant settings can be configured at the JVM level and should not involve changing encryption settings in
cassandra.yaml. See https://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/FIPS.html

For information on generating the keystore and truststore files used in SSL communications, see
http://download.oracle.com/javase/6/docs/technotes/guides/security/jsse/JSSERefGuide.html#CreateKeystore

Inter-node Encryption
~~~~~~~~~~~~~~~~~~~~~

The settings for managing inter-node encryption are found in ``cassandra.yaml`` in the ``server_encryption_options``
section. To enable inter-node encryption, change the ``internode_encryption`` setting from its default value of ``none``
to one value from: ``rack``, ``dc`` or ``all``.

Client to Node Encryption
~~~~~~~~~~~~~~~~~~~~~~~~~

The settings for managing client to node encryption are found in ``cassandra.yaml`` in the ``client_encryption_options``
section. There are two primary toggles here for enabling encryption, ``enabled`` and ``optional``.

- If neither is set to ``true``, client connections are entirely unencrypted.
- If ``enabled`` is set to ``true`` and ``optional`` is set to ``false``, all client connections must be secured.
- If both options are set to ``true``, both encrypted and unencrypted connections are supported using the same port.
  Client connections using encryption with this configuration will be automatically detected and handled by the server.

As an alternative to the ``optional`` setting, separate ports can also be configured for secure and unsecure connections
where operational requirements demand it. To do so, set ``optional`` to false and use the ``native_transport_port_ssl``
setting in ``cassandra.yaml`` to specify the port to be used for secure client communication.

Roles
^^^^^

Cassandra uses database roles, which may represent either a single user or a group of users, in both authentication and
permissions management. Role management is an extension point in Cassandra and may be configured using the
``role_manager`` setting in ``cassandra.yaml``. The default setting uses ``CassandraRoleManager``, an implementation
which stores role information in the tables of the ``system_auth`` keyspace.

See also: `CREATE ROLE <cql.html#create-role>`_, `ALTER ROLE <cql.html#alter-role>`_, `DROP ROLE <cql.html#drop-role>`_,
`GRANT ROLE <cql.html#grant-role>`_, `REVOKE ROLE <cql.html#revoke-role>`_

Authentication
^^^^^^^^^^^^^^

Authentication is pluggable in Cassandra and is configured using the ``authenticator`` setting in ``cassandra.yaml``.
Cassandra ships with two options included in the default distribution.

By default, Cassandra is configured with ``AllowAllAuthenticator`` which performs no authentication checks and therefore
requires no credentials. It is used to disable authentication completely. Note that authentication is a necessary
condition of Cassandra's permissions subsystem, so if authentication is disabled, effectively so are permissions.

The default distribution also includes ``PasswordAuthenticator``, which stores encrypted credentials in a system table.
This can be used to enable simple username/password authentication.

Enabling Password Authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before enabling client authentication on the cluster, client applications should be pre-configured with their intended
credentials. When a connection is initiated, the server will only ask for credentials once authentication is
enabled, so setting up the client side config in advance is safe. In contrast, as soon as a server has authentication
enabled, any connection attempt without proper credentials will be rejected which may cause availability problems for
client applications. Once clients are setup and ready for authentication to be enabled, follow this procedure to enable
it on the cluster.

Pick a single node in the cluster on which to perform the initial configuration. Ideally, no clients should connect
to this node during the setup process, so you may want to remove it from client config, block it at the network level
or possibly add a new temporary node to the cluster for this purpose. On that node, perform the following steps:

1. Open a ``cqlsh`` session and change the replication factor of the ``system_auth`` keyspace. By default, this keyspace
   uses ``SimpleReplicationStrategy`` and a ``replication_factor`` of 1. It is recommended to change this for any
   non-trivial deployment to ensure that should nodes become unavailable, login is still possible. Best practice is to
   configure a replication factor of 3 to 5 per-DC.

::

    ALTER KEYSPACE system_auth WITH replication = {'class': 'NetworkTopologyStrategy', 'DC1': 3, 'DC2': 3};

2. Edit ``cassandra.yaml`` to change the ``authenticator`` option like so:

::

    authenticator: PasswordAuthenticator

3. Restart the node.

4. Open a new ``cqlsh`` session using the credentials of the default superuser:

::

    cqlsh -u cassandra -p cassandra

5. During login, the credentials for the default superuser are read with a consistency level of ``QUORUM``, whereas
   those for all other users (including superusers) are read at ``LOCAL_ONE``. In the interests of performance and
   availability, as well as security, operators should create another superuser and disable the default one. This step
   is optional, but highly recommended. While logged in as the default superuser, create another superuser role which
   can be used to bootstrap further configuration.

::

    # create a new superuser
    CREATE ROLE dba WITH SUPERUSER = true AND LOGIN = true AND PASSWORD = 'super';

6. Start a new cqlsh session, this time logging in as the new_superuser and disable the default superuser.

::

    ALTER ROLE cassandra WITH SUPERUSER = false AND LOGIN = false;

7. Finally, set up the roles and credentials for your application users with `CREATE ROLE <cql.html#create-role>`_
   statements.

At the end of these steps, the one node is configured to use password authentication. To roll that out across the
cluster, repeat steps 2 and 3 on each node in the cluster. Once all nodes have been restarted, authentication will be
fully enabled throughout the cluster.

Note that using ``PasswordAuthenticator`` also requires the use of `CassandraRoleManager <#roles>`_.

See also: `Setting credentials for internal authentication <cql.html#setting-credentials-for-internal-authentication>`_,
`CREATE ROLE <cql.html#create-role>`_, `ALTER ROLE <cql.html#alter-role>`_, `ALTER KEYSPACE <cql.html#alter-keyspace>`_,
`GRANT PERMISSION <cql.html#grant-permission>`_,

Authorization
^^^^^^^^^^^^^

Authorization is pluggable in Cassandra and is configured using the ``authorizer`` setting in ``cassandra.yaml``.
Cassandra ships with two options included in the default distribution.

By default, Cassandra is configured with ``AllowAllAuthorizer`` which performs no checking and so effectively grants all
permissions to all roles. This must be used if ``AllowAllAuthenticator`` is the configured authenticator.

The default distribution also includes ``CassandraAuthorizer``, which does implement full permissions management
functionality and stores its data in Cassandra system tables.

Enabling Internal Authorization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Permissions are modelled as a whitelist, with the default assumption that a given role has no access to any database
resources. The implication of this is that once authorization is enabled on a node, all requests will be rejected until
the required permissions have been granted. For this reason, it is strongly recommended to perform the initial setup on
a node which is not processing client requests.

The following assumes that authentication has already been enabled via the process outlined in `Enabling Password
Authentication <#enabling-password-authentication>`_. Perform these steps to enable internal authorization across the
cluster:

1. On the selected node, edit ``cassandra.yaml`` to change the ``authorizer`` option like so:

::

    authorizer: CassandraAuthorizer

2. Restart the node.

3. Open a new ``cqlsh`` session using the credentials of a role with superuser credentials:

::

    cqlsh -u dba -p super

4. Configure the appropriate access privileges for your clients using `GRANT PERMISSION <cql.html#grant-permission>`_
   statements. On the other nodes, until configuration is updated and the node restarted, this will have no effect so
   disruption to clients is avoided.

::

    GRANT SELECT ON ks.t1 TO db_user;

5. Once all the necessary permissions have been granted, repeat steps 1 and 2 for each node in turn. As each node
   restarts and clients reconnect, the enforcement of the granted permissions will begin.

See also: `GRANT PERMISSION <cql.html#grant-permission>`_, `GRANT ALL <cql.html#grant-all>`_,
`REVOKE PERMISSION <cql.html#revoke-permission>`_

Caching
^^^^^^^

Enabling authentication and authorization places additional load on the cluster by frequently reading from the
``system_auth`` tables. Furthermore, these reads are in the critical paths of many client operations, and so has the
potential to severely impact quality of service. To mitigate this, auth data such as credentials, permissions and role
details are cached for a configurable period. The caching can be configured (and even disabled) from ``cassandra.yaml``
or using a JMX client. The JMX interface also supports invalidation of the various caches, but any changes made via JMX
are not persistent and will be re-read from ``cassandra.yaml`` when the node is restarted.

Each cache has 3 options which can be set:

Validity Period
    Controls the expiration of cache entries. After this period, entries are invalidated and removed from the cache.
Refresh Rate
    Controls the rate at which background reads are performed to pick up any changes to the underlying data. While these
    async refreshes are performed, caches will continue to serve (possibly) stale data. Typically, this will be set to a
    shorter time than the validity period.
Max Entries
    Controls the upper bound on cache size.

The naming for these options in ``cassandra.yaml`` follows the convention:

* ``<type>_validity_in_ms``
* ``<type>_update_interval_in_ms``
* ``<type>_cache_max_entries``

Where ``<type>`` is one of ``credentials``, ``permissions``, or ``roles``.

As mentioned, these are also exposed via JMX in the mbeans under the ``org.apache.cassandra.auth`` domain.

JMX access
^^^^^^^^^^

Access control for JMX clients is configured separately to that for CQL. For both authentication and authorization, two
providers are available; the first based on standard JMX security and the second which integrates more closely with
Cassandra's own auth subsystem.

The default settings for Cassandra make JMX accessible only from localhost. To enable remote JMX connections, edit
``cassandra-env.sh`` (or ``cassandra-env.ps1`` on Windows) to change the ``LOCAL_JMX`` setting to ``yes``. Under the
standard configuration, when remote JMX connections are enabled, `standard JMX authentication
<#standard-jmx-auth>`_ is also switched on.

Note that by default, local-only connections are not subject to authentication, but this can be enabled.

If enabling remote connections, it is recommended to also use `SSL <#jmx-with-ssl>`_ connections.

Finally, after enabling auth and/or SSL, ensure that tools which use JMX, such as `nodetool
<#nodetool-and-other-tooling>`_, are correctly configured and working as expected.

Standard JMX Auth
~~~~~~~~~~~~~~~~~

Users permitted to connect to the JMX server are specified in a simple text file. The location of this file is set in
``cassandra-env.sh`` by the line:

::

    JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.password.file=/etc/cassandra/jmxremote.password"

Edit the password file to add username/password pairs:

::

    jmx_user jmx_password

Secure the credentials file so that only the user running the Cassandra process can read it :

::

    $ chown cassandra:cassandra /etc/cassandra/jmxremote.password
    $ chmod 400 /etc/cassandra/jmxremote.password

Optionally, enable access control to limit the scope of what defined users can do via JMX. Note that this is a fairly
blunt instrument in this context as most operational tools in Cassandra require full read/write access. To configure a
simple access file, uncomment this line in ``cassandra-env.sh``:

::

    #JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.access.file=/etc/cassandra/jmxremote.access"

Then edit the access file to grant your JMX user readwrite permission:

::

    jmx_user readwrite

Cassandra must be restarted to pick up the new settings.

See also : `Using File-Based Password Authentication In JMX
<http://docs.oracle.com/javase/7/docs/technotes/guides/management/agent.html#gdenv>`_


Cassandra Integrated Auth
~~~~~~~~~~~~~~~~~~~~~~~~~

An alternative to the out-of-the-box JMX auth is to useeCassandra's own authentication and/or authorization providers
for JMX clients. This is potentially more flexible and secure but it come with one major caveat. Namely that it is not
available until `after` a node has joined the ring, because the auth subsystem is not fully configured until that point
However, it is often critical for monitoring purposes to have JMX access particularly during bootstrap. So it is
recommended, where possible, to use local only JMX auth during bootstrap and then, if remote connectivity is required,
to switch to integrated auth once the node has joined the ring and initial setup is complete.

With this option, the same database roles used for CQL authentication can be used to control access to JMX, so updates
can be managed centrally using just ``cqlsh``. Furthermore, fine grained control over exactly which operations are
permitted on particular MBeans can be acheived via `GRANT PERMISSION <cql.html#grant-permission>`_.

To enable integrated authentication, edit ``cassandra-env.sh`` to uncomment these lines:

::

    #JVM_OPTS="$JVM_OPTS -Dcassandra.jmx.remote.login.config=CassandraLogin"
    #JVM_OPTS="$JVM_OPTS -Djava.security.auth.login.config=$CASSANDRA_HOME/conf/cassandra-jaas.config"

And disable the JMX standard auth by commenting this line:

::

    JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.password.file=/etc/cassandra/jmxremote.password"

To enable integrated authorization, uncomment this line:

::

    #JVM_OPTS="$JVM_OPTS -Dcassandra.jmx.authorizer=org.apache.cassandra.auth.jmx.AuthorizationProxy"

Check standard access control is off by ensuring this line is commented out:

::

   #JVM_OPTS="$JVM_OPTS -Dcom.sun.management.jmxremote.access.file=/etc/cassandra/jmxremote.access"

With integrated authentication and authorization enabled, operators can define specific roles and grant them access to
the particular JMX resources that they need. For example, a role with the necessary permissions to use tools such as
jconsole or jmc in read-only mode would be defined as:

::

    CREATE ROLE jmx WITH LOGIN = false;
    GRANT SELECT ON ALL MBEANS TO jmx;
    GRANT DESCRIBE ON ALL MBEANS TO jmx;
    GRANT EXECUTE ON MBEAN 'java.lang:type=Threading' TO jmx;
    GRANT EXECUTE ON MBEAN 'com.sun.management:type=HotSpotDiagnostic' TO jmx;

    # Grant the jmx role to one with login permissions so that it can access the JMX tooling
    CREATE ROLE ks_user WITH PASSWORD = 'password' AND LOGIN = true AND SUPERUSER = false;
    GRANT jmx TO ks_user;

Fine grained access control to individual MBeans is also supported:

::

    GRANT EXECUTE ON MBEAN 'org.apache.cassandra.db:type=Tables,keyspace=test_keyspace,table=t1' TO ks_user;
    GRANT EXECUTE ON MBEAN 'org.apache.cassandra.db:type=Tables,keyspace=test_keyspace,table=*' TO ks_owner;

This permits the ``ks_user`` role to invoke methods on the MBean representing a single table in ``test_keyspace``, while
granting the same permission for all table level MBeans in that keyspace to the ``ks_owner`` role.

Adding/removing roles and granting/revoking of permissions is handled dynamically once the initial setup is complete, so
no further restarts are required if permissions are altered.

See also: `Permissions <cql.html#permissions>`_

JMX With SSL
~~~~~~~~~~~~

JMX SSL configuration is controlled by a number of system properties, some of which are optional. To turn on SSL, edit
the relevant lines in ``cassandra-env.sh`` (or ``cassandra-env.ps1`` on Windows) to uncomment and set the values of these
properties as required:

``com.sun.management.jmxremote.ssl``
    set to true to enable SSL
``com.sun.management.jmxremote.ssl.need.client.auth``
    set to true to enable validation of client certificates
``com.sun.management.jmxremote.registry.ssl``
    enables SSL sockets for the RMI registry from which clients obtain the JMX connector stub
``com.sun.management.jmxremote.ssl.enabled.protocols``
    by default, the protocols supported by the JVM will be used, override with a comma-separated list. Note that this is
    not usually necessary and using the defaults is the preferred option.
``com.sun.management.jmxremote.ssl.enabled.cipher.suites``
    by default, the cipher suites supported by the JVM will be used, override with a comma-separated list. Note that
    this is not usually necessary and using the defaults is the preferred option.
``javax.net.ssl.keyStore``
    set the path on the local filesystem of the keystore containing server private keys and public certificates
``javax.net.ssl.keyStorePassword``
    set the password of the keystore file
``javax.net.ssl.trustStore``
    if validation of client certificates is required, use this property to specify the path of the truststore containing
    the public certificates of trusted clients
``javax.net.ssl.trustStorePassword``
    set the password of the truststore file

See also: `Oracle Java7 Docs <http://docs.oracle.com/javase/7/docs/technotes/guides/management/agent.html#gdemv>`_,
`Monitor Java with JMX <https://www.lullabot.com/articles/monitor-java-with-jmx>`_


Nodetool (and other tooling)
----------------------------

.. todo:: Try to autogenerate this from Nodetool’s help.

Hardware Choices
----------------

Like most databases, Cassandra throughput improves with more CPU cores, more RAM, and faster disks. While Cassandra can
be made to run on small servers for testing or development environments (including Raspberry Pis), a minimal production
server requires at least 2 cores, and at least 8GB of RAM. Typical production servers have 8 or more cores and at least
32GB of RAM.

CPU
^^^
Cassandra is highly concurrent, handling many simultaneous requests (both read and write) using multiple threads running
on as many CPU cores as possible. The Cassandra write path tends to be heavily optimized (writing to the commitlog and
then inserting the data into the memtable), so writes, in particular, tend to be CPU bound. Consequently, adding
additional CPU cores often increases throughput of both reads and writes.

Memory
^^^^^^
Cassandra runs within a Java VM, which will pre-allocate a fixed size heap (java's Xmx system parameter). In addition to
the heap, Cassandra will use significant amounts of RAM offheap for compression metadata, bloom filters, row, key, and
counter caches, and an in process page cache. Finally, Cassandra will take advantage of the operating system's page
cache, storing recently accessed portions files in RAM for rapid re-use.

For optimal performance, operators should benchmark and tune their clusters based on their individual workload. However,
basic guidelines suggest:

-  ECC RAM should always be used, as Cassandra has few internal safeguards to protect against bit level corruption
-  The Cassandra heap should be no less than 2GB, and no more than 50% of your system RAM
-  Heaps smaller than 12GB should consider ParNew/ConcurrentMarkSweep garbage collection
-  Heaps larger than 12GB should consider G1GC

Disks
^^^^^
Cassandra persists data to disk for two very different purposes. The first is to the commitlog when a new write is made
so that it can be replayed after a crash or system shutdown. The second is to the data directory when thresholds are
exceeded and memtables are flushed to disk as SSTables.

Commitlogs receive every write made to a Cassandra node and have the potential to block client operations, but they are
only ever read on node start-up. SSTable (data file) writes on the other hand occur asynchronously, but are read to
satisfy client look-ups. SSTables are also periodically merged and rewritten in a process called compaction.  The data
held in the commitlog directory is data that has not been permanently saved to the SSTable data directories - it will be
periodically purged once it is flushed to the SSTable data files.

Cassandra performs very well on both spinning hard drives and solid state disks. In both cases, Cassandra's sorted
immutable SSTables allow for linear reads, few seeks, and few overwrites, maximizing throughput for HDDs and lifespan of
SSDs by avoiding write amplification. However, when using spinning disks, it's important that the commitlog
(``commitlog_directory``) be on one physical disk (not simply a partition, but a physical disk), and the data files
(``data_file_directories``) be set to a separate physical disk. By separating the commitlog from the data directory,
writes can benefit from sequential appends to the commitlog without having to seek around the platter as reads request
data from various SSTables on disk.

In most cases, Cassandra is designed to provide redundancy via multiple independent, inexpensive servers. For this
reason, using NFS or a SAN for data directories is an antipattern and should typically be avoided.  Similarly, servers
with multiple disks are often better served by using RAID0 or JBOD than RAID1 or RAID5 - replication provided by
Cassandra obsoletes the need for replication at the disk layer, so it's typically recommended that operators take
advantage of the additional throughput of RAID0 rather than protecting against failures with RAID1 or RAID5.

Common Cloud Choices
^^^^^^^^^^^^^^^^^^^^

Many large users of Cassandra run in various clouds, including AWS, Azure, and GCE - Cassandra will happily run in any
of these environments. Users should choose similar hardware to what would be needed in physical space. In EC2, popular
options include:

- m1.xlarge instances, which provide 1.6TB of local ephemeral spinning storage and sufficient RAM to run moderate
  workloads
- i2 instances, which provide both a high RAM:CPU ratio and local ephemeral SSDs
- m4.2xlarge / c4.4xlarge instances, which provide modern CPUs, enhanced networking and work well with EBS GP2 (SSD)
  storage

Generally, disk and network performance increases with instance size and generation, so newer generations of instances
and larger instance types within each family often perform better than their smaller or older alternatives.
