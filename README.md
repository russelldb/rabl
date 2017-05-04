Quick and dirty RabbitMQ Real Time Replication for Riak.

After unzipping the `.ez` files in `deps` compile with

    rebar3 compile

and when you run Riak, add `ebin` to the `advanced.conf` path args,
and set `ERL_LIBS=deps` before you run `riak start`
