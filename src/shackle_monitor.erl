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
    backlog_stats(),

    {ok, State#state {
        timer_ref = new_timer()
    }}.

%% private
backlog_metrics(_Client, []) ->
    ok;
backlog_metrics(Client, [{{PoolName, Index}, Backlog} | T]) ->
    PoolNameBin = atom_to_binary(PoolName, latin1),
    Key = <<PoolNameBin/binary, ".backlog." , (integer_to_binary(Index))/binary>>,
    ?METRICS(Client, gauge, Key, Backlog),
    backlog_metrics(Client, T).

backlog_stats() ->
    {ok, PoolInfo} = foil:all(shackle_pool),
    lists:foreach(fun
        ({PoolName, backlog} = Key) ->
            #pool_options {
                client = Client
            } = maps:get(PoolName, PoolInfo),
            Table = maps:get(Key, PoolInfo),
            backlog_metrics(Client, ets:tab2list(Table));
        (_) ->
            ok
    end, maps:keys(PoolInfo)).

new_timer() ->
    {_Mega, _Sec, Micro} = os:timestamp(),
    Delay = trunc((1000000 - Micro) / 1000),
    erlang:send_after(Delay, self(), reload).
