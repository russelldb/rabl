**WARNING WARNING, this is pre-release software that is under active development. Here be dragons (and rabbits.)**

# What is rabl?

Riak is a replicated distributed database that runs as a cluster. When
more than one cluster is needed, for example in multiple datacentres,
Riak's internal replication cannot be used over the WAN.

Rabl is an open source, "realtime" replication library for Riak
derived databases, which uses [rabbitmq](https://www.rabbitmq.com/) as
the queue and therefore amqp as the transport, between clusters.

There was a closed source, proprietary
[Multi-Datacentre Replication](http://docs.basho.com/riak/kv/2.2.3/using/reference/v2-multi-datacenter/architecture/)
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
replicate a changed object immediately after a successful write. If
for any reason the object failed to be replicated, tough. The "full
sync" replication run daily, or hourly, or whenever, would take care
of it.

Rabl just does this best effort, immediate replication, for now.

# Why did you make this?

NHS Digital use Riak for a number of critical services. They do not,
however, use Basho's full sync, as they found it error prone and
liable to negatively effect their services' performance. In order to
get NHS Digital off the closed source proprietary Riak Enterprise
codebase, and on to open source, only the realtime replication
functionality needs adding to open source Riak. That is why we made
rabl.

Rabl, in this initial and current form, is designed to be dropped into
a running Riak Enterprise or Riak OSS node. The aim is to minimize the
number of software transitions/upgrades that must take place to go
from Riak Enterprise to open source Riak. Once rabl is handling
realtime replication for a cluster, rolling upgrades to OSS Riak can
take place.

# Why RabbitMQ?

Very quickly it became clear that one possible design for realtime
replication was to add objects to a queue to be consumed on remote
clusters. Though using a queue made sense, writing one did
not. RabbitMQ is a stable, well used, mature, open source, Erlang,
queue implementation which NHS-Digital have experience and expertise
running.

Using RabbitMQ gives rabl a lot:

- Want more reliability? Rabbit supports clustering, ACKs, delivery
  reports, dead letter queues, blocked connection handling etc.
- Want more security? Rabbit supports SSL,
  authentication/authorisation/accounting.
- Want more monitoring? Rabbit has management and monitoring plug-ins.
- Want to handle special network conditions? Rabbit has shovel,
  support for NAT, network tuning options etc.

It makes no sense to develop all this for rabl when it has been
expertly developed and run in the wild for years by the many RabbitMQ
contributors and users.

# How it does it?

Details of the design and inner workings are in
[docs/design.md](). The short version is that each Riak node needs a
rabbitmq node installing alongside it, though you could just as well
use a pair of rabbitmq clusters and
[shovel](https://www.rabbitmq.com/shovel.html).

Rabl is made up of producers, consumers, and a post-commit hook.

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
options means that the post-commit hook is not invoked on the
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

### Get rabl

clone rabl and build it

         git clone https://github.com/russelldb/rabl.git
         cd rabl
         rebar3 compile

### Setup rabl

Configure rabl in Riak's `advanced.config`. There is an
[example config](config/sys.config) in the `config` directory of this
project. It is commented. Essentially you tell rabl, by means of
[amqp urls](https://www.rabbitmq.com/uri-query-parameters.html) how to
connect to the local rabbitmq node, and any/all remote rabbitmq nodes.

### Start Riak with rabl

Start Riak with rabl's libraries. You can either drop all the contents
of `_build/default/lib/` into your Riak lib (for example
`/usr/lib64/riak/lib/`) OR start Riak with `ERL_LIBS` pointing at
`rabl`:

         ERL_LIBS=/Path/To/rabl/_build/default/lib/ riak start

### Start rabbitmq

Start your rabbitmq node(s). Rabl will try and connect as soon as it
is started. If it can't connect the consumers will poll indefinitely,
with back-off (see config), while the producer will poll until it
gives up and thereafter attempt a reconnect when it is called by Riak.

### Start rabl

Start rabl. There is a file `rablctl.escript` in `bin/` that should
work, but is under development. It uses riak's `escript`
[command](https://www.tiot.jp/riak-docs/riak/kv/2.2.3/using/admin/riak-cli/#escript). All
the following commands use the same basic format.

        riak escript FULL_PATH_TO/rablctl.escript NODE COOKIE COMMAND [ARG]

If for any reason `rablctl.escript` does not work for you, see [riak attach below](#riak-atach). Start rabl with

        riak escript PATH/rablctl.escript riak@host riak start

You should just get `ok` as a response. You need to start rabl on EACH
and EVERY riak node that you want to replicate to and from.

You can Check that rabl started OK by running

        riak escript PATH/rablctl.escript riak@host riak status

 This will show the connection status of the consumers and
 producers. Look for `connected` for the producer(s), and `consuming`
 for the consumer(s)

### Add the rabl hook

Add the rabl hook to a bucket:

        riak escript PATH/rablctl.escript riak@host riak add-hook test


### Rablicate!

A quick way to test that rabl replication is working is to attach to
the riak node and call `rabl_util:put(<<"test">>, <<"key1">>,
<<"val1">>).` on one cluster, and then get the object on the other
cluster.

### Stats

If you want to see how it is going, you can call

         riak escript PATH/rablctl.escript riak@host riak stats

to see some latency histograms and counters from rabl.

#### riak attach

If for any reason the rablctl.escript doesn't work for you, you can use `riak attach` and run the following commands:

    rabl_app:start(). %% start rabl
    rabl_app:status(). %% see worker status
    rabl_util:add_hook(<<"test">>). %% add hook to bucket <<"test">>
    rabl_util:get_flat_stats(). %% see stats
    application:stop(rabl). %% stop rabl
