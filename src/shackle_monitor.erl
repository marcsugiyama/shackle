-module(shackle_monitor).
-include("shackle_internal.hrl").

-export([
    start_link/1
]).

-behavior(metal).
-export([
    init/3,
    handle_msg/2
]).

-record(state, {
    timer_ref :: reference()
}).

-define(RELOAD_INTERVAL, 5000). % 5 seconds

%% public
-spec start_link(server_name()) ->
    {ok, pid()}.

start_link(Name) ->
    metal:start_link(?MODULE, Name, undefined).

%% metal callbacks
-spec init(server_name(), pid(), server_opts()) ->
    {ok, term()}.

init(_Name, _Parent, _Opts) ->
    {ok, #state {
        timer_ref = new_timer()
    }}.

-spec handle_msg(term(), term()) ->
    {ok, term()}.

handle_msg(reload, State) ->
    {ok, PoolsInfo} = foil:all(shackle_pool),

    backlog_stats(PoolsInfo),
    queue_stats(PoolsInfo),

    {ok, State#state {
        timer_ref = new_timer()
    }}.

%% private
backlog_metrics(_Client, []) ->
    ok;
backlog_metrics(Client, [{{Pool, Index}, Backlog} | T]) ->
    Key = key(Pool, <<"backlog.", (integer_to_binary(Index))/binary>>),
    ?METRICS(Client, gauge, Key, Backlog),
    backlog_metrics(Client, T).

backlog_stats(PoolsInfo) ->
    lists:foreach(fun
        ({Pool, backlog} = Key) ->
            #pool_options {
                client = Client
            } = maps:get(Pool, PoolsInfo),
            Table = maps:get(Key, PoolsInfo),
            backlog_metrics(Client, ets:tab2list(Table));
        (_) ->
            ok
    end, maps:keys(PoolsInfo)).

key(Pool, Metric) ->
    PoolBin = atom_to_binary(Pool, latin1),
    <<"monitor.", PoolBin/binary, ".", Metric/binary>>.

new_timer() ->
    {Mega, Sec, Micro} = os:timestamp(),
    Unix = (Mega * 1000000000 + Sec * 1000) + trunc(Micro / 1000),
    Delta = Unix rem ?RELOAD_INTERVAL,
    erlang:send_after(Delta, self(), reload).

queue_stats(PoolsInfo) ->
    lists:foreach(fun (Pool) ->
        #pool_options {
            client = Client
        } = maps:get(Pool, PoolsInfo),
        Table = queue_table_name(Pool),
        Size = ets:info(Table, size),
        Key = key(Pool, <<"queue">>),
        ?METRICS(Client, gauge, Key, Size)
    end, maps:keys(PoolsInfo)).

queue_table_name(PoolName) ->
    list_to_atom("shackle_queue_" ++ atom_to_list(PoolName)).
