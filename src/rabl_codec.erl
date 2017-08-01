%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%% encode/decode rabl messages
%%% @end
%%% Created : 31 Jul 2017 by Russell Brown <russell@wombat.me>

-module(rabl_codec).

-compile(export_all).

-spec encode(erlang:timestamp(), rabl_riak:bk(), binary()) ->
                    {ok, binary()}.
encode(Time, BK, BinObj) ->
    Bin = term_to_binary({Time, BK, BinObj}),
    {ok, Bin}.

-spec decode(binary()) ->
                    {ok, {erlang:timestamp(),
                     {rabl_riak:bucket(), rabl_riak:key()},
                     BinObj::binary()}} |
                    {error, Reason::term()}.
decode(Binary) ->
    try
        {Time, BK, BinObj} = binary_to_term(Binary),
        {ok, {Time, BK, BinObj}}
    catch _A:B ->
            {error, B}
    end.


