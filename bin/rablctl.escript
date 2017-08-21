-module(riak_metrics).
-compile(export_all).

-define(CMDS, [load, show_config, 'show-config',
               start, add_hook, 'add-hook',
               setup, stop, status, stats]).

main(ArgsList) ->
    {NodeName, Cookie, Command, Args} = parse_args(ArgsList),
    LocalName = 'rablctl@127.0.0.1',
    case net_kernel:start([LocalName]) of
        {ok, _} ->
            erlang:set_cookie(node(), Cookie),
            case net_kernel:hidden_connect_node(NodeName) of
                true ->
                    io:format("calling  ~p with ~p~n", [Command, Args]),
                    Res = rpc:call(NodeName, rabl_console, Command, Args),
                    io:format("~p~n", [Res]),
                    ok;
                false ->
                    io:format("Could not connect to ~s with cookie ~s",
                              [NodeName, Cookie]);
                _ ->
                    io:format("net_kernel:connect/1 reports ~s is not alive",
                              [LocalName])
            end;
        {error, Reason} ->
            io:format("Could not connect node: ~w~n", [Reason])
    end.

halt_with_usage() ->
    usage(),
    halt(1).

usage() ->
    io:format("Usage: riak escript rablctl NODENAME COOKIE
    COMMAND=[load | show-config | start | add-hook <<BUCKET>> |
                  setup | stop | status | stats]~n").

parse_args([NodeName0, Cookie0, Command0]) ->
    parse_args([NodeName0, Cookie0, Command0, []]);
parse_args([NodeName0, Cookie0, Command0, Args0]) ->
    NodeName = list_to_atom(NodeName0),
    Cookie = list_to_atom(Cookie0),
    Command = get_command(list_to_atom(Command0)),
    Args = get_args(Command, Args0),
    {NodeName, Cookie, Command, Args}.

get_command(Command) ->
    case lists:member(Command, ?CMDS) of
        true ->
            normalise_command(Command);
        false ->
            halt_with_usage()
    end.

normalise_command('add-hook') ->
    add_hook;
normalise_command('show-config') ->
    show_config;
normalise_command(Cmd) ->
    Cmd.

get_args(add_hook, Bucket) ->
    [list_to_binary(Bucket)];
get_args(_Cmd, _) ->
    [].

