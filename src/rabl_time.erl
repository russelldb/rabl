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

monotonic_time() ->
    erlang:monotonic_time(millisecond).
