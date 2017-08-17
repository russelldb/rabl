%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%% reused functions for testing without a real rabbitmq
%%% @end
%%% Created :  4 Jul 2017 by Russell Brown <russell@wombat.me>

-module(rabl_mock).

-ifdef(TEST).
-compile(export_all).
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

mock_rabl_con(Caller) ->
    Pid = spawn(fun mock_rabl_loop/0),
    Caller ! {Caller, {opened_connection, Pid}},
    Pid.

mock_rabl_chan(Caller) ->
    Pid = spawn(fun mock_rabl_loop/0),
    Caller ! {Caller, {opened_channel, Pid}},
    Pid.

mock_rabl_loop() ->
    receive
        _Msg ->
            mock_rabl_loop()
    end.

%% the mock connection is a process (just like the real connection)
%% and the meck expect fun sends the pid to the test process. We block
%% to receive it.
receive_connection(SelfPid) ->
    receive
        {SelfPid, {opened_connection, ConPid}} ->
            ConPid
    after 5000 -> exit({timeout, connection})
    end.

%% the mock channel is a process, and the meck expect function sends
%% the pid to the test process
receive_channel(SelfPid) ->
    receive
        {SelfPid, {opened_channel, ChanPid}} ->
            ChanPid
    after 5000 -> exit({timeout, channel})
    end.

flush() ->
    flush(0).

flush(N) ->
    receive
        _Msg ->
            flush(N+1)
    after 0 ->
            ?debugFmt("flushed ~p messages~n", [N]),
            ok
    end.

%% generate a rabbit connection blocked message
amqp_blocked_message() ->
    #'connection.blocked'{reason= <<"test">>}.

%% generate a rabbit connection unblocked message
amqp_unblocked_message() ->
    #'connection.unblocked'{}.

-endif.


