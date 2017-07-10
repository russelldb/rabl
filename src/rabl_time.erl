%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%
%%% encapsulate time and timing functions for simpler mocking and
%%% testing
%%
%%% @end
%%% Created :  3 Jul 2017 by Russell Brown <russell@wombat.me>

-module(rabl_time).

-export([monotonic_time/0]).

-compile(nowarn_deprecated_function).

monotonic_time() ->
    try
        erlang:monotonic_time(millisecond)
    catch error:undef ->
            {MS, S, US} = erlang:now(),
            (MS*1000000+S)*1000000+US
    end.
