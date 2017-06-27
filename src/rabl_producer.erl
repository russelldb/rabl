%%%-------------------------------------------------------------------
%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc

%%% bound the number of channels created for rabl_hooks, this is
%%% pretty primative, and serial, probably need something better
%%% (pools/sidejob/cxy_fount etc) ALSO @TODO how to handle no produce
%%% connections, just crash? @TODO robustness, failure to open
%%% connection isn't a disaster.

%%% @end
%%% Created : 12 Jun 2017 by Russell Brown <russell@wombat.me>
%%%-------------------------------------------------------------------
-module(rabl_producer).

-behaviour(gen_server).

%% API
-export([start_link/0, get_channel/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {channels=[], counter=1}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Gets a channel to send a message on
%%
%% @end
%%--------------------------------------------------------------------
-spec get_channel() -> {ok, pid()} | {error, Reason::term()}.
get_channel() ->
    gen_server:call({local, ?SERVER}, get_channel, 5000).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    %% we don't want to crash when a connection closes, we just want
    %% to know, and open another
    process_flag(trap_exit, true),
    Producers = application:get_env(rabl, producer_count, 10),
    Channels= [start_link_local_conn() || _N <- lists:seq(1, Producers)],
    {ok, #state{channels=Channels}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(get_channel, _From, State) ->
    #state{channels=Channels, counter=Cnt} = State,
    Channel = lists:nth(Cnt, Channels),
    NewCount = increment_counter(Cnt, Channels),
    {reply, {ok, Channel}, State#state{counter=NewCount}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({'EXIT', Channel, Reason} , State) ->
    %% remove `Channel' from state, reopen a new one, log a warning,
    %% (change the counter?)  @TODO what if all channels close and we
    %% cannot re-open another? Should we crash???
    lager:error("Channel exited with reason ~p~n", [Reason]),
    #state{channels=Channels} = State,
    Channels2 = lists:delete(Channel, Channels),
    NewChannel = start_link_local_conn(),
    {noreply, State#state{channels= [NewChannel | Channels2]}};
handle_info(Other, State) ->
    lager:warning("Unexpected Info message ~p~n", [Other]),
    {noreply, State}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%% % Internal functions
%% %===================================================================
%% @private create a connection to the local rabbitmq, and link to it
-spec start_link_local_conn() -> [pid()].
start_link_local_conn() ->
    Channel = rabl_channel:start_local(),
    true = link(Channel),
    Channel.

%% @private increment the round robin counter
-spec increment_counter(pos_integer(), list(pid())) -> pos_integer().
increment_counter(Cnt, Channels) when Cnt >= length(Channels) ->
    1;
increment_counter(Cnt, _Channels) ->
    Cnt+1.
