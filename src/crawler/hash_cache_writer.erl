%%
%% hash_cache_writer.erl
%% Kevin Lynx
%% 07.17.2013
%% cache received hashes and pre-process theses hashes before inserted into database
%%
-module(hash_cache_writer).
-include("db_common.hrl").
-include("vlog.hrl").
-behaviour(gen_server).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).
-export([start_link/5, stop/0, insert/1]).
-record(state, {cache_time, cache_max, db_conn}).
-define(TBLNAME, hash_table).
% -define(DBPOOL, hash_write_db).
% -define(BATCH_INSERT, 100).

start_link(IP, Port, DBConn, MaxTime, MaxCnt) ->
	gen_server:start_link({local, srv_name()}, ?MODULE, [IP, Port, DBConn, MaxTime, MaxCnt], []).

stop() ->
	gen_server:cast(srv_name(), stop).

insert(Hash) when is_list(Hash) ->
	gen_server:cast(srv_name(), {insert, Hash}).

srv_name() ->
	?MODULE.

init([IP, Port, DBConn, MaxTime, Max]) ->
	% mongo_sup:start_pool(?DBPOOL, DBConn, {IP, Port}),
    Database = <<"dht_system">>,
    {ok, Conn} = mc_worker_api:connect([{database, Database},
                                        {host, IP},
                                        {port, Port}]),
	ets:new(?TBLNAME, [set, named_table]),
	{ok, #state{cache_time = 1000 * MaxTime, cache_max = Max, db_conn = Conn}, 0}.

terminate(_, State) ->
	% mongo_sup:stop_pool(?DBPOOL),
    mc_worker_api:disconnect(State#state.db_conn),
    {ok, State}.

code_change(_, _, State) ->
    {ok, State}.

handle_cast({insert, Hash}, State) ->
	do_insert(Hash),
	try_save(State),
	{noreply, State};

handle_cast(stop, State) ->
	{stop, normal, State}.

handle_call(_, _From, State) ->
	{noreply, State}.

handle_info(do_save_cache, #state{cache_time = Time, db_conn = Conn} = State) ->
	?I("timeout to save cache hashes"),
	do_save_merge(table_size(?TBLNAME), Conn),
	schedule_save(Time),
	{noreply, State};

handle_info(timeout, #state{cache_time = Time} = State) ->
	schedule_save(Time),
	{noreply, State}.

schedule_save(Time) ->
	timer:send_after(Time, do_save_cache).

%%
do_insert(Hash) when is_list(Hash) ->
	NewVal = 1 + get_req_cnt(Hash),
	ets:insert(?TBLNAME, {Hash, NewVal}).

table_size(Tbl) ->
	Infos = ets:info(Tbl),
	proplists:get_value(size, Infos).

try_save(#state{cache_max = Max, db_conn = Conn}) ->
	TSize = table_size(?TBLNAME),
	try_save(TSize, Max, Conn).

try_save(Size, Max, Conn) when Size >= Max ->
	?I(?FMT("try save all cache hashes ~p", [Size])),
	do_save_merge(Size, Conn);
try_save(_, _, _) ->
	ok.

%% new method
%% merge hashes into database, to decrease hashes processed by hash_reader
do_save_merge(0,_) ->
	ok;
do_save_merge(_, Conn) ->
	First = ets:first(?TBLNAME),
	ReqAt = time_util:now_seconds(),
	{ReqSum, NewSum} = do_save(First, ReqAt, Conn),
	% Conn = mongo_pool:get(?DBPOOL),
	db_system:stats_cache_query_inserted(Conn, NewSum),
	db_system:stats_query_inserted(Conn, ReqSum),
	ets:delete_all_objects(?TBLNAME).

do_save('$end_of_table', _, _) ->
	{0, 0};
do_save(Key, ReqAt, Conn) ->
    %mongo_pool:get(?DBPOOL),
	ReqCnt = get_req_cnt(Key),
	BHash = list_to_binary(Key),
	% Cmd = {findAndModify, ?HASH_COLLNAME, query, {'_id', BHash},
	% 	update, {'$inc', {req_cnt, ReqCnt}, '$set', {req_at, ReqAt}},
	%	fields, {'_id', 1}, upsert, true, new, false},
    Cmd = #{<<"$inc">> => #{<<"req_cnt">> => ReqCnt},
            <<"$set">> => #{<<"req_at">> => ReqAt}},
	Ret = mc_worker_api:update(#{connection =>Conn, collection => ?HASH_COLLNAME,
                                 selector => #{<<"_id">> => BHash},
                                 doc => Cmd, upsert => true, database => ?HASH_DBNAME}),
    % Ret = mongo:do(safe, master, Conn, ?HASH_DBNAME, fun() ->
	% 	mongo:command(Cmd)
	% end),
	New = case Ret of
              {true, #{<<"n">> := 1}} ->
	%	{value, _Obj, lastErrorObject, {updatedExisting, true, n, 1}, ok, 1.0} ->
                  ?I(?FMT("do_save update ~p", [BHash])),
                  0;
              _ ->
                  1
          end,
	Next = ets:next(?TBLNAME, Key),
	{ReqSum, NewSum} = do_save(Next, ReqAt, Conn),
	{ReqCnt + ReqSum, New + NewSum}.

get_req_cnt(Hash) ->
	case ets:lookup(?TBLNAME, Hash) of
		[{Hash, ReqCnt}] -> ReqCnt;
		[] -> 0
	end.
