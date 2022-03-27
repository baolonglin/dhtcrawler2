%%
%% crawler_app.erl
%% Kevin Lynx
%% 06.19.2013
%%
-module(crawler_app).
-behaviour(application).
-export([start/2, stop/1]).
-export([start/0, stop/0]).

% application behaviour callback
start(_Type, _StartArgs) ->
	config:start_link("dhtcrawler.config", fun() -> config_default() end),
	do_start().

stop(_State) ->
	config:stop(),
	crawler_sup:stop().

do_start() ->
	StartPort = config:get(start_port),
	Count = config:get(node_count),
	LogLevel = config:get(loglevel),
	DBConn = config:get(dbconn),
	DBHost = config:get(dbhost),
	DBPort = config:get(dbport),
	CacheMaxCount = config:get(hash_max_cache),
	CacheMaxTime = config:get(cache_max_time),
	io:format("dhtcrawler startup ~p, ~p, ~p:~p~n", [StartPort, Count, DBHost, DBPort]),
	crawler_sup:start_link({StartPort, Count, DBHost, DBPort, LogLevel, DBConn, CacheMaxTime, CacheMaxCount}).

start() ->
	filelib:ensure_dir("log/"),
	error_logger:logfile({open, "log/crash.log"}),
	code:add_path("_build/default/lib/bson/ebin"),
	code:add_path("_build/default/lib/kdht/ebin"),
	code:add_path("_build/default/lib/mongodb/ebin"),
	code:add_path("_build/default/lib/poolboy/ebin"),
	code:add_path("_build/default/lib/pbkdf2/ebin"),
	code:add_path("_build/default/lib/giza/ebin"),
	code:add_path("_build/default/lib/ibrowse/ebin"),
	Apps = [asn1, crypto, public_key, ssl, inets, bson, pbkdf2, poolboy],
	[application:start(App) || App <- Apps],
    application:ensure_all_started(mongodb),
	application:start(dhtcrawler).

stop() ->
	application:stop(dhtcrawler).

config_default() ->
	[{start_port, 6776},
	 {node_count, 50},
	 {hash_max_cache, 300},
	 {cache_max_time, 2*60}, % seconds
	 {loglevel, 3},
	 {dbconn, 5},
	 {dbhost, "localhost"},
	 {dbport, 27017}].

