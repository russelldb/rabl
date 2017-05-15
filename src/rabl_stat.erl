%%%-------------------------------------------------------------------
%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%% record stats for publish, consume for latency tracking
%%% @end
%%% Created : 11 May 2017 by Russell Brown <russell@wombat.me>
%%%-------------------------------------------------------------------
-module(rabl_stat).

-behaviour(gen_server).

%% API
-export([start_link/0, publish/3, consume/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-type bucket_key() :: {binary(), binary()} | {{binary(), binary()}, binary()}.
-type timing_tag() :: any(). %% whatever you want for correlating across nodes

-record(state, {publish_log, consume_log, log_dir}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, Pid::pid()} | ignore | {error, Error::term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Called when a hook publishes a riak_object
%%%% @end
%%--------------------------------------------------------------------
-spec publish(bucket_key(), timing_tag(), erlang:timestamp()) -> ok.
publish(BK, Tag, Timestamp) ->
    gen_server:cast(?MODULE, {publish, BK, Tag, Timestamp}).

%%--------------------------------------------------------------------
%% @doc
%% Called when a consumer recieves an riak_object
%%%% @end
%%--------------------------------------------------------------------
-spec consume(bucket_key(), timing_tag(), erlang:timestamp()) -> ok.
consume(BK, Tag, Timestamp) ->
    gen_server:cast(?MODULE, {consume, BK, Tag, Timestamp}).

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
    %% this probably needs thinking about, eh?
    LogDir = application:get_env(rabl, log_dir, "/tmp/rabl_log/"),
    ok = filelib:ensure_dir(LogDir),
    {ok, PubLog} = file:open(filename:join([LogDir, publish]), [append]),
    {ok, ConLog} = file:open(filename:join([LogDir, consume]), [append]),
    {ok, #state{publish_log=PubLog, consume_log=ConLog, log_dir=LogDir}}.

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
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

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
handle_cast({publish, BK, Tag, Timestamp}, State) ->
    #state{publish_log=PubLog} = State,
    ok = io:fwrite(PubLog, "~p ~p ~p~n", [BK, Tag, Timestamp]),
    {noreply, State};
handle_cast({consume, BK, Tag, Timestamp}, State) ->
    #state{consume_log=ConLog} = State,
    ok = io:fwrite(ConLog, "~p ~p ~p~n", [BK, Tag, Timestamp]),
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
handle_info(_Info, State) ->
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
terminate(_Reason, State) ->
    #state{consume_log=ConLog, publish_log=PubLog} = State,
    file:close(ConLog),
    file:close(PubLog),
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
%%% Internal functions
%%%===================================================================
