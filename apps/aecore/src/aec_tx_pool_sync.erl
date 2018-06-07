%%% -*- erlang-indent-level:4; indent-tabs-mode: nil -*-
%%%-------------------------------------------------------------------
%%% @copyright (C) 2018, Aeternity Anstalt
%%% @doc Memory pool synchronization
%%%
-module(aec_tx_pool_sync).

-behaviour(gen_server).

%% API
-export([ start_link/0
        , stop/1
        ]).

-export([ sync_get/2
        , sync_unfold/2
        ]).


%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { tree = undefined }).

%% -- API --------------------------------------------------------------------
start_link() ->
    gen_server:start_link(?MODULE, [], []).

stop(Sync) ->
    gen_server:stop(Sync).

sync_get(Sync, TxHashes) ->
    gen_server:call(Sync, {get, TxHashes}).

sync_unfold(Sync, Unfolds) ->
    gen_server:call(Sync, {unfold, Unfolds}).

%% -- gen_server callbacks ---------------------------------------------------

init([]) ->
    self() ! init_tree,
    {ok, #state{}}.

handle_call({get, TxHashes}, _From, State) ->
    {reply, {ok, do_get(TxHashes)}, State};
handle_call({unfold, Unfolds}, _From, State = #state{ tree = Tree }) ->
    NewUnfolds = lists:append([ unfold(Unfold, Tree) || Unfold <- Unfolds ]),
    {reply, {ok, NewUnfolds}, State};
handle_call(Request, From, State) ->
    lager:warning("Ignoring unknown call request from ~p: ~p", [From, Request]),
    {noreply, State}.

handle_cast(Msg, State) ->
    lager:warning("Ignoring unknown cast message: ~p", [Msg]),
    {noreply, State}.

handle_info(init_tree, _) ->
    F = fun(TxHash, Tree) ->
            aeu_mp_trees:put(TxHash, [], Tree)
        end,
    Tree = aec_db:fold_mempool(F, aeu_mp_trees:new()),
    lager:debug("ZZ tree built", []),
    {noreply, #state{ tree = Tree }};
handle_info(Info, State) ->
    lager:warning("Ignoring unknown info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -- Local functions --------------------------------------------------------

do_get(TxHashes) ->
    lists:concat([ do_get_(Tx) || Tx <- TxHashes ]).

do_get_(TxHash) ->
    try
        STx = aec_db:get_signed_tx(TxHash),
        [STx]
    catch _:_ ->
        []
    end.

unfold({node, Path, Node}, Tree) ->
    aeu_mp_trees:unfold(Path, Node, Tree);
unfold({subtree, Path}, Tree) ->
    get_subtree(Path, Tree);
unfold({key, Key}, _Tree) ->
    [{key, Key}].

get_subtree(Key, _Tree) when byte_size(Key) =:= 32 ->
    [Key];
get_subtree(Path, Tree) ->
    get_subtree(aeu_mp_trees:iterator_from(Path, Tree), Path, bit_size(Path), []).

get_subtree(Iter, Path, S, Acc) ->
    case aeu_mp_trees:iterator_next(Iter) of
        {Key = <<Path:S/bits, _Rest>>, [], NewIter} ->
            get_subtree(NewIter, Path, S, [{key, Key} | Acc]);
        _ ->
            lists:reverse(Acc)
    end.
