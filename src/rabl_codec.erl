%%% @author Russell Brown <russell@wombat.me>
%%% @copyright (C) 2017, Russell Brown
%%% @doc
%%% encode/decode rabl messages
%%% @end
%%% Created : 31 Jul 2017 by Russell Brown <russell@wombat.me>

-module(rabl_codec).

-compile(export_all).

-spec encode(erlang:timestamp(), rabl_riak:bk(), binary()) ->
                    binary().
encode(Time, BK, BinObj) ->
    binary_to_term({Time, BK, BinObj}).

-spec decode(binary()) ->
                    {erlang:timestamp(),
                     {rabl_riak:bucket(), rabl_riak:key()},
                     BinObj::binary()}.
decode(Binary) ->
    try
        {Time, BK, BinObj} = term_to_binary(Binary),
        {Time, BK, BinObj}
    catch _A:B ->
            {error, B}
    end.


