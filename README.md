# What is rabl?

Riak is a replicated distributed database that runs as a cluster. When
more than one cluster is needed, for example in multiple datacentres,
Riak's internal replication cannot be used over the WAN.

Rabl is an open source, "realtime" replication library for Riak
derived databases, which uses [rabbitmq]() as the queue and therefore
amqp as the transport, between clusters.

There was a closed source, proprietary Multi-Datacentre Replication
product sold by Riak's developers, Basho. This product provided both
"full sync" and "realtime" replication between Riak clusters in
different datacentres. Rabl only addresses realtime replication.

The name Rabl was chosen as replication was often referred to as
"repl" at Basho. Rabl is shortening of the concatenation
RabbitMQ-Replication. There is a function in rabl called `rablicate` :D

Basho's product had a lot of bells and whistles, with multiple
clusters and complex routes and topologies between them. At the time
of writing rabl is for two clusters (though adding more should be
easy.) The clusters can both be serving requests and replicating to
the other, or run in active-passive mode, it's up to you.


## Realtime? Replication?

The term "realtime replication" is how Basho described their
product. What they meant was a system that would make a best effort to
replicate a changed object immediatley after a successful write. If
for any reason the object failed to be replicated, tough. The "full
sync" replication run daily, or hourly, or whenever, would take care
of it.

Rabl just does this best effort, immediate replication, for now.

# Why did you make this?

NHS Digital use Riak for a number of critical services. They do not,
however, use Basho's full sync, as they found it error prone and
liable to negatively effect their services' performance. In order to
get NHS Digital off the closed source propriatery Riak Enterprise
codebase, and on to open source, only the realtime replication
functionality needs adding to open source Riak. That is why we made
rabl.

Rabl, in this initial and current form, is designed to be dropped into
a running Riak Enterprise or Riak OSS node. The aim is to minimise the
number of software transitions/upgrades that must take place to go
from Riak Enterprise to open source Riak. Once rabl is handling
realtime replication for a cluster, rolling upgrades to OSS Riak can
take place.

# How it does it?

Details of the design and inner workings are in
[docs/design.md](). The short version is that each Riak node needs a
rabbitmq node installing alongside it, though you could just as well
use a pair of rabbitmq clusters and
[shovel](https://www.rabbitmq.com/shovel.html).

Rabl is made up of producers, consumers, and a postcommit hook.

A
[post-commit hook](http://docs.basho.com/riak/kv/2.2.3/developing/usage/commit-hooks/)
is added to those buckets you want to replicate. When the hook is
called rabl sends a message containing the object to the rabbitmq
instance configured in the `producers` section of the config. The
producer publishes to a queue named after the cluster. This is the
`cluster_name` setting in the config. The number of producers you need
per node can be discovered through testing. Not that many is a good
rule of thumb.

On the remote cluster each Riak node runs a number of consumers. In
the tested configuration we have 5 nodes, each with 5*5
consumers. That is 5 consumers for the rabbitmq queue on
SourceCluster:Node1, 5 for SourceCluster:Node2 and so on. Again, you
may find it simpler to configure 2 rabbitmq clusters and a shovel,
though we found this does not perform as well as the 1 isolated
rabbitmq node per Riak node. Each consumer subscribes to the
`sink_queue`, this should be the same value as the remote
`cluster_name`.

When a consumer receives a rabbitmq message it unpacks it and uses a
local `riak_client` to store the object in Riak. Using the correct put
options means that the postcommit hook is not invoked on the
replicated PUT and short circuits the object going around and around
in replication forever.

# How do I use it?

WARNING WARNING, this is pre-release software that is under active
development. Here be dragons (and rabbits.)

Eventually we plan for rabl to be an included application of Riak
OSS. For now you need to sort of drop it in to your existing
deployment.

The steps below will be cleaned up and made more user friendly, but
for now, if you want to try rabl, here is what you must do:

1. clone rabl and build it

         git clone https://github.com/russelldb/rabl.git
         cd rabl
         rebar3 compile

1. Configure rabl in Riak's `advanced.config`. There is an
[example config](config/sys.config) in the `config` directory of this
project. It is commented. Essentially you tell rabl, by means of
[amqp urls](https://www.rabbitmq.com/uri-query-parameters.html) how to
connect to the local rabbitmq node, and any/all remote rabbitmq nodes.

1. Start Riak with rabl's libraries. You can either drop all the contents of `_build/default/lib/` into your Riak lib (for example `/usr/lib64/riak/lib/`) OR start Riak with `ERL_LIBS` pointing at `rabl`:

         ERL_LIBS=/Path/To/rabl/_build/default/lib/ riak start

1. Start your rabbitmq node(s). Rabl will try and connect as soon as
it is started. If it can't connect the consumers will poll
indefinitaly, with back-off (see config), while the producer will poll
until it gives up and thereafter attempt a reconnect when it is called
by Riak.

1. Start rabl. There is a file `rablctl` in `bin/` that may work, but is under development. Drop in your riak installs `bin` directory and call

        rablctl start

    If that doesn't work, then the other option is to connect to riak and start it manualy:

        riak attach
        1> rabl_app:start().

1. Check that rabl started OK by running either `rablctl status` or, if attached `rabl_app:status()`. This will show tha connection status of the consumers and producers.

1. Add the rabl hook to a bucket. `rablctl add-hook test` or if
attached `rabl_util:add_hook(<<"test">>).` will do it.

1. Rablicate! A quick way to test this, if attached would be to call
`rabl_util:put(<<"test">>, <<"key1">>, <<"val1">>).` on one cluster,
and then get the object on the other cluster.

1. If you want to see how it is going, you can call `rablctl stats`
or, if attached `rabl_util:get_flat_stats().` To see some latency
histograms and counters from rabl.
