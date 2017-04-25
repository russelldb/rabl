rabl
=====

Quick and dirty RabbitMQ Real Time Replication for Riak.

After unzipping the `.ez` files in `deps` compile with

    ERL_LIBS=deps erlc src/*.erl -o ebin

and when you run Riak, add `ebin` to the `advanced.conf` path args,
and set `ERL_LIBS=deps` before you run `riak start`
