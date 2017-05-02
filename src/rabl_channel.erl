%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%% Super simple (thanks Jon Brisbin!) way to get rabbitmq channels
%%% @end
%%% Created :  2 May 2017 by Russell Brown <russell@wombat.me>

-module(rabl_channel).

-compile(export_all).

-define(RABL_PG, rabl).

get() ->
    case pg2:get_closest_pid(?RABL_PG) of
        {error, {no_such_group, ?RABL_PG}} ->
            pg2:create(?RABL_PG),
            %% 'cos get is an inbuilt BIF
            ?MODULE:get();
        {error, {no_process, ?RABL_PG}} ->
            start();
        Pid ->
            {ok, Pid}
    end.

%% start and "register" in pg2, assumes exchange, q, binding all exist
-spec start() -> {ok, pid()} | {error, Reason::term()}.
start() ->
    Conn = rabl:connect(),
    Channel = rabl:open_channel(Conn),
    pg2:join(?RABL_PG, Channel),
    {ok, Channel}.

add(Channel) ->
    pg2:join(?RABL_PG, Channel).

close(Channel) ->
    rabl:close_channel(Channel).

get_all() ->
    case pg2:get_members(?RABL_PG) of
        {error, _Meh} ->
            [];
        Pids when is_list(Pids) ->
            Pids
    end.
