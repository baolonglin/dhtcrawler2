%%
%% db_hash_index.erl
%% Kevin Lynx
%% 07.14.2013
%%
-module(db_hash_index).
-export([insert/2, exist/2]).
-define(DBNAME, <<"hash_cache">>).
-define(COLLNAME, <<"hashes">>).

insert(Conn, BHash) when is_binary(BHash) ->
	case catch do_insert(Conn, BHash) of
		{'EXIT', _} -> failed;
		_ -> ok
	end.

do_insert(Conn, BHash) ->
    mc_worker_api:insert(#{connection => Conn, colleciton => ?COLLNAME,
                          database => ?DBNAME,
                          doc => #{<<"_id">> => BHash}}).
	% Doc = {'_id', list_to_binary(Hash)},
	% mongo:do(safe, master, Conn, ?DBNAME, fun() ->
	%	mongo:insert(?COLLNAME, Doc)
	%end).

exist(Conn, BHash) when is_binary(BHash) ->
	% Sel = {'_id', list_to_binary(Hash)},
	% Doc = mongo:do(safe, master, Conn, ?DBNAME, fun() ->
	%	mongo:find_one(?COLLNAME, Sel)
	% end),
    Doc = mc_worker_api:find_one(#{connection => Conn, collection => ?COLLNAME,
                                   database => ?DBNAME,
                                   selector => #{<<"_id">> => BHash}}),
	Doc /= {}.

