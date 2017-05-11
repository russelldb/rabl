%%%-------------------------------------------------------------------
%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%% control rabl in riak
%%% @end
%%% Created :  9 May 2017 by Russell Brown <russell@wombat.me>
%%%-------------------------------------------------------------------
-module(rabl_escript).

%% API exports
-export([main/1]).
-compile(export_all).

%%====================================================================
%% API functions
%%====================================================================

%% escript Entry point
main(Args) ->
    {ParsedArgs, ExtraArgs} = case getopt:parse(cli_opts(), Args) of
                                  {ok, {P, E}} -> {P, E};
                                  _ -> usage()
                              end,
    io:format("Parsed Args ~p Opts ~p ~n", [ParsedArgs, ExtraArgs]),

    case run_usage(ParsedArgs) of
        true -> usage();
        _ -> ok
    end,

    Conf = read_conf(proplists:get_value(config, ParsedArgs, undefined), ParsedArgs),

    %%ok = start_lager(Conf),
    {ok, RiakNode} = start_node(Conf),

    {ok, Action} = get_action(ParsedArgs),
    perform_action(Action, ParsedArgs, RiakNode),

    erlang:halt(0).

%%====================================================================
%% Internal functions
%%====================================================================
cli_opts() ->
    [
     {action,       undefined,  undefined,     atom,                "action to be performed"},
     {bucket,       $b,         "bucket",      binary,                   "bucket for add-hook"},
     {config,       $c,         "config",       {string, "rablctl.conf"}, "config file containing riak node/cookie"},
     {help,         $h,         "help",         undefined,                "help / usage"}
    ].

usage() ->
    getopt:usage(cli_opts(), "rablctl"),
    halt(0).

run_usage([]) -> true;
run_usage(ParsedArgs) ->
    lists:member(help, ParsedArgs).

read_conf(undefined, Args) ->
    run_usage(Args);
read_conf(File, _Args) ->
    case file:consult(File) of
        {ok, Conf} ->
            Conf;
        Error ->
            io:format("Error ~p reading config file ~p~n", [Error, File]),
            halt(0)
    end.

%% decide what we're doing with rabl-in-riak
get_action(ParsedArgs) ->
    case proplists:get_value(action, ParsedArgs) of
        undefined ->
            io:format("Please specify an 'action' [start | add-hook]~n"),
            usage();
        Action when Action == start; Action == 'add-hook'; Action == consumers ->
            {ok, Action}
    end.

perform_action(start, _ParsedArgs, RiakNode) ->
    case rpc:call(RiakNode, rabl_app, start, []) of
        ok ->
            io:format("rabl started on ~p~n", [RiakNode]);
        {error, {already_started,rabl}} ->
            io:format("rabl already started on ~p~n", [RiakNode]);
        {error, Reason} ->
            io:format("rabl maybe failed to start on ~p with error ~p~n", [RiakNode, Reason]);
        {badrpc, Reason} ->
            io:format("attmept to start rabl on ~p failed with badrpc error ~p~n", [RiakNode, Reason])
    end;
perform_action('add-hook', ParsedArgs, RiakNode) ->
    Bucket = proplists:get_value(bucket, ParsedArgs),
    ok = rpc:call(RiakNode, rabl_util, add_hook, [Bucket]),
    BucketProps = rpc:call(RiakNode, riak_core_bucket, get_bucket, [Bucket]),
    io:format("rabl hook added to ~p.~n~p~n", [Bucket, BucketProps]);
perform_action('consumers', _ParsedArgs, RiakNode) ->
    case rpc:call(RiakNode, supervisor, which_children, [rabl_sup]) of
        Children when is_list(Children) ->
            io:format("rabl consumers:~n ~p~n", [Children]);
        Error ->
            io:format("Failed to get consumer list with ~p~n", [Error])
end.

start_node(Conf) ->
    CtlNode = proplists:get_value(rablctl_nodename, Conf, 'rablctl@127.0.0.1'),
    CtlCookie = proplists:get_value(rablctl_cookie, Conf, rablctl),
    RiakCookie = proplists:get_value(riak_cookie, Conf, riak),
    RiakNode = proplists:get_value(riak_node, Conf, 'riak@127.0.0.1'),
    [] = os:cmd("epmd -daemon"),
    net_kernel:start([CtlNode]),
    erlang:set_cookie(CtlNode, CtlCookie),
    erlang:set_cookie(RiakNode, RiakCookie),
    case net_kernel:hidden_connect_node(RiakNode) of
        true -> {ok, RiakNode};
        false ->
            io:format("unable to connect to riak node ~p with cookie ~p~n", [RiakNode, RiakCookie]),
            erlang:halt(0)
    end.
