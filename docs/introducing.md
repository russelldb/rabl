# Introducing Rabl

This post is about rabl, a [RabbitMQ]() backed, open source drop in
replacement for Basho's "realtime" multi-datacentre replication
product.

NOTE: I started work on this post before
[bet365](https://www.bet365.com) dropped
[this bombshell](http://lists.basho.com/pipermail/riak-users_lists.basho.com/2017-August/019500.html)
on the
[riak users mailing list](http://lists.basho.com/mailman/listinfo/riak-users_lists.basho.com). With
the impending open sourcing of all Basho's proprietary code maybe this
post and rabl are redundant, but maybe not. Read on for details.

## MDC - Full Sync and Realtime

Basho's [Riak]() database is Apache 2 Licensed open source software,
but their multi-datacentre replication (hereafter MDC) add on, dubbed
RiakEE, was a paid for, propriteary, closed source product. It
provided two key inter-acting features, "Realtime Replication" and
"Full Sync Replication". Realtime is a process that makes a best
endeavour to send any changed object to some configured remote
cluster(s) as soon as the write is committed at the local
cluster. Full Sync, as the name suggests, runs at some regular
interval and ensures that any pair of clusters are up-to-date with
each other and contain all the same data.

### Why Open Source MDC?

[Basho recently declared bankrupty]() and went into
receivership. Shortly prior to this, clearly in financial dire
straits, Basho started offering for sale a "perpertual license" to its
enterprise customers, allowing them to continue to use MDC in
perpetuity. However permsission to use the closed source code, (even
with access to the code, as many have through an escrow clause in
their contracts) is not the same as running open source software. I
won't try and rehash all the arguments for open source here.

There is a large and active community of industrial Riak users, and
without Basho to develop and maintain Riak, an open source alternative
to replication is needed if the community is to benefit from continued
development effort on a common code base. More than that, though, my
current client NHS-Digital don't use Basho's Fullsync replication as
it has failed to meet their expectations. Realtime replication has
also been a cause for concern. When sophisticated users like the NHS
have a problem with Riak they can usually create a branch of the code,
and make the changes they want (a large amount of functionality in
Riak's Secondary Index code was contributed by the NHS), but they
could not fix MDC replication as they had no access to the source

### Why is Rabl Realtime Only?

As mentioned above, the NHS have been less than satisfied with Basho's
fullsync product. They use realtime replication coupled with a
technique they developed for fullsync called "delta touch". Delta
Touch exploits the observation that fullsync is only needed to mop up
any changes that somehow failed to be replicated in realtime.

#### Delta-Touch

Every time an object is written an index entry of the last modified
time and an index that is a serial change number for the object is
updated. The indexes are queried regularly (every few minutes), and
for varying time intervals (in terms of modified dates).  For example
frequently query for the past hour, but occasionally query for a
longer time period, with the the longer the time period the less
frequent the query.  The varied query time periods is because they
don't want to miss deltas because the query wasn't running for a
period, or because they appeared on the index after the lastmod\_date
had passed.

Both clusters run the delta-touch queries, but only the more
up-to-date site prompts the "touch", which is an object update to
provoke realtime replication of "touched" objects. If a source side
sees the sink is more up-to-date it logs an error, which is picked up
in a chart, and human monitoring should detect a situation where
deltas are being discovered but not fixed.

NHS-Digital are working on a new and different fullsync mechanism,
(some hints
[here](https://github.com/martinsumner/leveled/blob/master/docs/ANTI_ENTROPY.md))
but it is not ready yet. I still consider it a worthwhile enterprise
given the general perception that Basho's product was flawed and
neglected. Maybe in the light of recent events it makes more sense to
focus on the existing codebase, only time and a look at the code will
tell. If nothing else an open, side-by-side comparison will at least
now be possible.

#### Realtime, All The Time

At the time of writing NHS-Digital only use realtime replication and
so only need a realtime replication replacement to move to open source
Riak.

Upgrading a running Riak cluster with a large amount of data can be
time consuming. We want to minimise the number of operational events
at NHS-Digital. Rabl is designed so that it can deployed without a
node upgrade. A simple transistion of adding the rabl and rabbit libs,
updating the advanced.config, and restarting Riak is all it takes to
deploy rabl. This means there are very few steps to take NHS-Digital
from proprietary RiakEE to Open Source Riak with realtime replication.

### Why RabbitMQ?

Working on the principal of the easiest possible thing that could
work, a first design for realtime replication could be a Riak
[post-commit hook](https://www.tiot.jp/riak-docs/riak/kv/2.2.3/developing/usage/commit-hooks/#post-commit-hooks)
and a queue. When an object is written, the post-commit hook publishes
the changed object to the queue, and a subscriber to the queue on a
remote cluster reads from the queue and writes the changed object.

There exists a perfectly capable Erlang queue implementation in
[RabbitMQ](https://www.rabbitmq.com/). RabbitMQ has a lot going for
it:

* NHS-Digital already use RabbitMQ extensively
* RabbitMQ has a long history of successful deployment (10 years!)
* RabbitMQ is Open Source
* There is an Active Community

And beyond that, why write a queue when a good enough queue exists?
RabbitMQ means less rabl code. RabbitMQ also gives the deployer a lot
of flexibility:

* Want more reliability? Rabbit supports clustering, ACKs, delivery
  reports, dead letter queues, blocked connection handling etc.
* Want more security? Rabbit supports SSL,
  authentication/authorisation/accounting.
* Want more monitoring? Rabbit has management and monitoring plug-ins.
* Want to handle special network conditions? Rabbit has shovel,
  support for NAT, network tuning options etc.

We were able to run a proof of concept realtime replication
implementation after a few hours work, and see that yes, RabbitMQ
could do the job.

### How Rabl works

As described above, the easiest thing that could work is a post-commit
hook, a queue, and a subscriber, and that is essentially all there is
to rabl.

The queue is RabbitMQ. In testing what has worked best for us has been
a RabbitMQ node per Riak node. We tested with two RabbitMQ clusters
using shovel for the multi-datacenter replication and found the
latency variability to be extremely high. With a RabbitMQ node per
Riak node we've seen more predictable performance.

Although not my best diagram, what I attempted to show is that each
Riak node publishes changes to its local RabbitMQ node and consumes
from every remote RabbitMQ node. Though the topology is configurable
and up to you.

![A RabbitMQ Node Per Riak Node](many-2-many-2.png "A rabbit per riak")

Rabl is an OTP application. At present it is designed to be started
inside a Riak node that is running unmodified Basho code (Enterprise
or Open Source) though in future it could be included with Riak. Each
Riak node in the cluster runs rabl. Rabl is not a distributed or
clustered application.

![rabl process hierarchy](rabl.png "rabl process hierarchy")

The rabl app is made up of two sides, producers and consumers. A
configurable number of each is declared in the advanced.config for
riak, in the `rabl` tuple.

Both consumers and producers are declared in a two-tuple of `{Type, [{Count,
AMQPUrl}]}` for example:

        {rabl, [
            {consumers, [{3, "amqp://remote1"}, {3, "amqp://remote2"}]},
            {producers, [{1, "amqp://localhost"}]}
            ]}.

means "start 3 consumers of the queue on remote1 and 3 from remote2"
and 1 producer to the local RabbitMQ node. When rabl starts up the
consumer supervisor will start 6 consumer processes, 3 to remote1 and
3 to remote2. These processes will each encapsulate a connection to
the remote RabbitMQ node and a local Riak Client. Likewise the
producer supervisor will start a single producer that wraps a local
RabbitMQ connection. When a remote Riak node publishes a changed Riak
object to its queue via the `rabl_hook` post-commit hook, one of the
consumers will be notified by RabbitMQ and store the object
locally. Setting the correct PUT option parameters on the PUT at the
consumer ensures that the post-commit hook doesn't fire again and
short-circuits and eternal loop of replication.

Rabl currently only supports a simple 2 cluster topology. In the rabl
configuration a local `clustername` is declared, and that is the queue
to which Riak publishes changes. A `source_cluster` is also declared,
and this is the queue to which rabl subscribes. Extending rabl for
multiple clusters and queues is as simple as changing the format of
the configuration.

### Trying Rabl

Rather than reproduce the instructions in the README, I direct you to
the
[How Do I Use It](https://github.com/nhs-riak/rabl#how-do-i-use-it)
section.

### Stats and Results

Rabl use [folsom](https://github.com/boundary/folsom) to generate some
basic metrics. Rabl uses local time at each cluster to calculate
latency. I know! I know! distributed systems and time! If the
clusters' clocks aren't reasonably well synchronised your latency
histograms will be meaningless.

So far the testing has been fairly simple and based soley around the
NHS-Digital use case. The chart below shows latencies for
one-way-replication from cluster ONE to cluster TWO. The NHS use case
has fairly large objects. The load generator puts objects into Riak on
cluster ONE. There are small objects, sized between 1k and 100k with a
random distribution, and large objects, sized between 500k and 1.5mb
with a random distribution. The ratio of small objects to large
objects is 100:1.

![One Way Replication Latency](one-way-repl.png "One Way Repl Latency")

The environment is two 4 node Riak clusters on r3x.larges, one in
us-west-1 the other in us-west-2. Though not in any way conclusive,
this gives and impression of the latency for these larger objects.

### Future Work

Well that really is the question now, isn't it? Originally this
section was about further testing and benchmarking and tuning,
polishing and production readying code, through to deployment at the
NHS. Now, with bet365 open sourcing MDC replication, I guess the next
obvious step would be a side-by-side comparision. That list of
RabbitMQ advantages above haven't gone away just because the Basho
code is open source. I didn't work on MDC replication at Basho, but
I'm fairly sure that the realtime queuing code cannot be as feature
rich as RabbitMQ. Watch this space, I guess.

## Conclusion

The simplest possible thing that could work _worked_ well enough in
testing. RabbitMQ is perfectly capable for this kind of use
case. Hooking an OTP application into a Riak node is a simple way to
drop realtime replication into an existing set up. Though the future
of rabl is now far from clear, it's open source and currently
developed and you're welcome to give the tyres a kick
[https://github.com/nhs-riak/rabl](https://github.com/nhs-riak/rabl).
