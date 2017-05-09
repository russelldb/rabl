%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%% Super simple (thanks Jon Brisbin!) way to get rabbitmq channels
%%% @end
%%% Created :  2 May 2017 by Russell Brown <russell@wombat.me>

-module(rabl_channel).

-compile(export_all).

-define(RABL_PG, rabl).

%% @doc find a channel and return it.
-spec get() -> {ok, pid()}.
get() ->
    case pg2:get_closest_pid(?RABL_PG) of
        {error, {no_such_group, ?RABL_PG}} ->
            pg2:create(?RABL_PG),
            %% 'cos `get' is an inbuilt BIF
            ?MODULE:get();
        {error, {no_process, ?RABL_PG}} ->
            start();
        Pid ->
            {ok, Pid}
    end.

%% start and "register" in pg2, assumes exchange, q, binding all exist
%% @see rabl_util:setup_rabl/0
-spec start() -> {ok, pid()} | {error, Reason::term()}.
start() ->
    {ok, Host} = application:get_env(rabl, rabbit_host),
    Conn = rabl:connect(Host),
    Channel = rabl:open_channel(Conn),
    ok = join(Channel),
    {ok, Channel}.

%% @doc add given `Channel' to register
-spec add(pid()) -> ok.
add(Channel) when is_pid(Channel) ->
    join(Channel).

%% @doc close `Channel'
-spec close(pid()) -> ok.
close(Channel) when is_pid(Channel) ->
    rabl:close_channel(Channel).

%% @doc get all registered channels
-spec get_all() -> [pid()].
get_all() ->
    case pg2:get_members(?RABL_PG) of
        {error, _Meh} ->
            [];
        Pids when is_list(Pids) ->
            Pids
    end.

%% @private internal de-deuplication
-spec join(pid()) -> ok.
join(Channel) ->
    case pg2:join(?RABL_PG, Channel) of
        {error, {no_such_group, ?RABL_PG}} ->
            pg2:create(?RABL_PG),
            join(Channel);
        ok ->
            ok
    end.
