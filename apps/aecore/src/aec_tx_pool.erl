%%% -*- erlang-indent-level:4; indent-tabs-mode: nil -*-
%%%-------------------------------------------------------------------
%%% @copyright (C) 2018, Aeternity Anstalt
%%% @doc Memory pool of unconfirmed transactions.
%%%
%%% Unconfirmed transactions are transactions not included in any
%%% block in the longest chain.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(aec_tx_pool).

-behaviour(gen_server).

-define(MEMPOOL, mempool).
-define(KEY_NONCE_PATTERN(Sender), {{'_', Sender, '$1', '_'}, '_'}).

%% API
-export([ start_link/0
        , stop/0
        ]).

-export([ get_candidate/2
        , get_max_nonce/1
        , peek/1
        , push/1
        , push/2
        , size/0
        , top_change/2
        ]).

-export([ sync_abort/2
        , sync_finish/2
        , sync_get/2
        , sync_init/1
        , sync_start/1
        , sync_unfold/2
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(TAB, ?MODULE).

-record(state,
        { db :: pool_db()
        , sync = none :: none | {active, term(), term()} | in_sync
        , sync_out = [] :: [{sync, binary(), pid()}]
        }).

-type negated_fee() :: non_pos_integer().
-type non_pos_integer() :: neg_integer() | 0.

-type pool_db_key() ::
        {negated_fee(), aec_keys:pubkey(), non_neg_integer(), binary()}.
-type pool_db_value() :: aetx_sign:signed_tx().
-type pool_db() :: atom().

-type event() :: tx_created | tx_received.

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() ->
    %% implies also clearing the mempool
    gen_server:stop(?SERVER).

-define(PUSH_EVENT(E), Event =:= tx_created; Event =:= tx_received).

%% INFO: Transaction from the same sender with the same nonce and fee
%%       will be overwritten
-spec push(aetx_sign:signed_tx()) -> ok.
push(Tx) ->
    push(Tx, tx_created).

-spec push(aetx_sign:signed_tx(), event()) -> ok.
push(Tx, Event) when ?PUSH_EVENT(Event) ->
    %% Verify that this is a signed transaction.
    try aetx_sign:tx(Tx)
    catch _:_ -> error({illegal_transaction, Tx})
    end,
    gen_server:call(?SERVER, {push, Tx, Event}).

-spec get_max_nonce(aec_keys:pubkey()) -> {ok, non_neg_integer()} | undefined.
get_max_nonce(Sender) ->
    gen_server:call(?SERVER, {get_max_nonce, Sender}).

%% The specified maximum number of transactions avoids requiring
%% building in memory the complete list of all transactions in the
%% pool.
-spec peek(pos_integer() | infinity) -> {ok, [aetx_sign:signed_tx()]}.
peek(MaxN) when is_integer(MaxN), MaxN >= 0; MaxN =:= infinity ->
    gen_server:call(?SERVER, {peek, MaxN}).

-spec get_candidate(pos_integer(), binary()) -> {ok, [aetx_sign:signed_tx()]}.
get_candidate(MaxN, BlockHash) when is_integer(MaxN), MaxN >= 0,
                                    is_binary(BlockHash) ->
    gen_server:call(?SERVER, {get_candidate, MaxN, BlockHash}).

-spec top_change(binary(), binary()) -> ok.
top_change(OldHash, NewHash) ->
    gen_server:call(?SERVER, {top_change, OldHash, NewHash}).

-spec size() -> non_neg_integer() | undefined.
size() ->
    ets:info(?MEMPOOL, size).

-spec sync_start(PeerId :: binary()) -> ok.
sync_start(PeerId) ->
    gen_server:cast(?SERVER, {sync_start, PeerId}).

-spec sync_init(PeerId :: binary()) -> ok | {error, term()}.
sync_init(PeerId) ->
    gen_server:call(?SERVER, {sync_init, PeerId}).

-spec sync_abort(PeerId :: binary(), Error :: {error, term()}) -> ok.
sync_abort(PeerId, Error) ->
    gen_server:call(?SERVER, {sync_abort, PeerId, Error}).

-spec sync_finish(PeerId :: binary(), done | {error, term()}) -> ok.
sync_finish(PeerId, Error) ->
    gen_server:call(?SERVER, {sync_finish, PeerId, Error}).

-spec sync_get(PeerId :: binary(), TxHashes :: [binary()]) ->
        {ok, [aetx_sign:signed_tx()]} | {error, term()}.
sync_get(PeerId, TxHashes) ->
    case gen_server:call(?SERVER, {sync_pid, PeerId}) of
        {ok, Pid} ->
            lager:debug("ZZZ get ~p at ~p", [TxHashes, Pid]),
            aec_tx_pool_sync:sync_get(Pid, TxHashes);
        Err = {error, _} ->
            Err
    end.

-spec sync_unfold(PeerId :: binary(), Unfolds :: [binary()]) ->
        {ok, [aetx_sign:signed_tx()]} | {error, term()}.
sync_unfold(PeerId, SerUnfolds) ->
    case gen_server:call(?SERVER, {sync_pid, PeerId}) of
        {ok, Pid} ->
            Unfolds = deserialize_unfolds(SerUnfolds),
            lager:debug("ZZZ unfold ~p at ~p", [Unfolds, Pid]),
            {ok, NewUnfolds} = aec_tx_pool_sync:sync_unfold(Pid, Unfolds),
            lager:debug("ZZZ got result ~p at ~p", [NewUnfolds, Pid]),
            {ok, serialize_unfolds(NewUnfolds)};
        Err = {error, _} ->
            Err
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, Db} = pool_db_open(),
    Handled  = ets:new(init_tx_pool, [private]),
    InitF  = fun(TxHash, _) ->
                     update_pool_on_tx_hash(TxHash, Db, Handled),
                     ok
             end,
    ok = aec_db:ensure_transaction(fun() -> aec_db:fold_mempool(InitF, ok) end),
    {ok, #state{db = Db}}.

handle_call({get_max_nonce, Sender}, _From, #state{db = Mempool} = State) ->
    {reply, int_get_max_nonce(Mempool, Sender), State};
handle_call({push, Tx, Event}, _From, #state{db = Mempool} = State) ->
    {reply, pool_db_put(Mempool, pool_db_key(Tx), Tx, Event), State};
handle_call({top_change, OldHash, NewHash}, _From,
            #state{db = Mempool} = State) ->
    do_top_change(OldHash, NewHash, Mempool),
    {reply, ok, State};
handle_call({peek, MaxNumberOfTxs}, _From, #state{db = Mempool} = State)
  when is_integer(MaxNumberOfTxs), MaxNumberOfTxs >= 0;
       MaxNumberOfTxs =:= infinity ->
    Txs = pool_db_peek(Mempool, MaxNumberOfTxs),
    {reply, {ok, Txs}, State};
handle_call({get_candidate, MaxNumberOfTxs, BlockHash}, _From,
            #state{db = Mempool} = State) ->
    Txs = int_get_candidate(MaxNumberOfTxs, Mempool, BlockHash),
    {reply, {ok, Txs}, State};
handle_call({sync_init, PeerId}, _From, State) ->
    handle_sync_init(State, PeerId);
handle_call({sync_abort, PeerId, _Error}, _From, State) ->
    {reply, ok, do_sync_abort(State, PeerId)};
handle_call({sync_finish, PeerId, Result}, _From, State) ->
    {reply, ok, do_sync_finish(State, PeerId, Result)};
handle_call({sync_pid, PeerId}, _From, State = #state{ sync_out = SyncOuts }) ->
    lager:debug("ZZZ sync_pid for ~p", [PeerId]),
    Res =
        case lists:keyfind(PeerId, 2, SyncOuts) of
            false       -> {error, no_sync};
            {_, _, Pid} -> {ok, Pid}
        end,
    {reply, Res, State};
handle_call(Request, From, State) ->
    lager:warning("Ignoring unknown call request from ~p: ~p", [From, Request]),
    {noreply, State}.

handle_cast({sync_start, PeerId}, State = #state{ sync = none }) ->
    lager:debug("ZZZ sync_start for ~p", [PeerId]),
    {noreply, sync_start(State, PeerId)};
handle_cast({sync_start, _PeerId}, State) ->
    {noreply, State};
handle_cast(Msg, State) ->
    lager:warning("Ignoring unknown cast message: ~p", [Msg]),
    {noreply, State}.

handle_info({'ETS-TRANSFER', _, _, _}, State) ->
    {noreply, State};
handle_info(Info, State) ->
    lager:warning("Ignoring unknown info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec int_get_max_nonce(pool_db(), aec_keys:pubkey()) -> {ok, non_neg_integer()} | undefined.
int_get_max_nonce(Mempool, Sender) ->
    case lists:flatten(ets:match(Mempool, ?KEY_NONCE_PATTERN(Sender))) of
        [] ->
            undefined;
        Nonces ->
            MaxNonce = lists:max(Nonces),
            {ok, MaxNonce}
    end.

%% Ensure ordering of tx nonces in one account, and for duplicate account nonces
%% we only get the one with higher fee.
int_get_candidate(MaxNumberOfTxs, Mempool, BlockHash) ->
    int_get_candidate(MaxNumberOfTxs, Mempool, ets:first(Mempool),
                      BlockHash, gb_trees:empty()).

int_get_candidate(0,_Mempool, _,_BlockHash, Acc) ->
    gb_trees:values(Acc);
int_get_candidate(_N,_Mempool, '$end_of_table',_BlockHash, Acc) ->
    gb_trees:values(Acc);
int_get_candidate(N, Mempool, {_Fee, Account, Nonce, _} = Key, BlockHash, Acc) ->
    case gb_trees:is_defined({Account, Nonce}, Acc) of
        true ->
            %% The earlier must have had higher fee. Skip this tx.
            int_get_candidate(N, Mempool, ets:next(Mempool, Key), BlockHash, Acc);
        false ->
            Tx = ets:lookup_element(Mempool, Key, 2),
            case check_nonce_at_hash(Tx, BlockHash) of
                ok ->
                    NewAcc = gb_trees:insert({Account, Nonce}, Tx, Acc),
                    Next = ets:next(Mempool, Key),
                    int_get_candidate(N - 1, Mempool, Next, BlockHash, NewAcc);
                {error, _} ->
                    %% This is not valid anymore.
                    %% TODO: This should also be deleted, but we don't know for
                    %% sure that we are still on the top.
                    int_get_candidate(N, Mempool, ets:next(Mempool, Key),
                                      BlockHash, Acc)
            end
    end.

-spec pool_db_key(aetx_sign:signed_tx()) -> pool_db_key().
pool_db_key(SignedTx) ->
    Tx = aetx_sign:tx(SignedTx),
    %% INFO: Sort by fee
    %%       TODO: sort by fee, then by origin, then by nonce

    %% INFO: * given that nonce is an index of transactions for a user,
    %%         the following key is unique for a transaction
    %%       * negative fee places high profit transactions at the beginning
    %%       * ordered_set type enables implicit overwrite of the same txs
    {-aetx:fee(Tx), aetx:origin(Tx), aetx:nonce(Tx), aetx_sign:hash(SignedTx)}.

-spec select_pool_db_key_by_hash(pool_db(), binary()) -> {ok, pool_db_key()} | not_in_ets.
select_pool_db_key_by_hash(Mempool, TxHash) ->
    case sel_return(ets_select(Mempool, [{ {{'$1', '$2', '$3', '$4'}, '_'},
                                           [{'=:=','$4', TxHash}],
                                           [{{'$1', '$2', '$3', '$4'}}] }], infinity)) of
        [Key] -> {ok, Key};
        [] -> not_in_ets
    end.

-spec pool_db_open() -> {ok, pool_db()}.
pool_db_open() ->
    {ok, ets:new(?MEMPOOL, [ordered_set, public, named_table])}.

-spec pool_db_peek(pool_db(), MaxNumber::pos_integer() | infinity) ->
                          [pool_db_value()].
pool_db_peek(_, 0) -> [];
pool_db_peek(Mempool, Max) ->
    sel_return(
      ets_select(Mempool, [{ {'_', '$1'}, [], ['$1'] }], Max)).

ets_select(T, P, infinity) ->
    ets:select(T, P);
ets_select(T, P, N) when is_integer(N), N >= 1 ->
    ets:select(T, P, N).

sel_return(L) when is_list(L) -> L;
sel_return('$end_of_table' ) -> [];
sel_return({Matches, _Cont}) -> Matches.

do_top_change(OldHash, NewHash, Mempool) ->
    %% Add back transactions to the pool from discarded part of the chain
    %% Mind that we don't need to add those which are incoming in the fork
    {ok, Ancestor} = aec_chain:find_common_ancestor(OldHash, NewHash),
    Handled = ets:new(foo, [private, set]),
    update_pool_from_blocks(Ancestor, OldHash, Mempool, Handled),
    update_pool_from_blocks(Ancestor, NewHash, Mempool, Handled),
    ets:delete(Handled),
    ok.

update_pool_from_blocks(Hash, Hash,_Mempool,_Handled) -> ok;
update_pool_from_blocks(Ancestor, Current, Mempool, Handled) ->
    lists:foreach(fun(TxHash) ->
                          update_pool_on_tx_hash(TxHash, Mempool, Handled)
                  end,
                  aec_db:get_block_tx_hashes(Current)),
    Prev = aec_chain:prev_hash_from_hash(Current),
    update_pool_from_blocks(Ancestor, Prev, Mempool, Handled).

update_pool_on_tx_hash(TxHash, Mempool, Handled) ->
    case ets:member(Handled, TxHash) of
        true -> ok;
        false ->
            ets:insert(Handled, {TxHash}),
            Tx = aec_db:get_signed_tx(TxHash),
            case aec_db:is_in_tx_pool(TxHash) of
                false ->
                    %% Added to chain
                    case select_pool_db_key_by_hash(Mempool, TxHash) of
                        {ok, Key} -> ets:delete(Mempool, Key);
                        not_in_ets -> pass
                    end;
                true ->
                    ets:insert(Mempool, {pool_db_key(Tx), Tx})
            end
    end.

-spec pool_db_put(pool_db(), pool_db_key(), aetx_sign:signed_tx(), event()) ->
                         'ok' | {'error', atom()}.
pool_db_put(Mempool, Key, Tx, Event) ->
    Hash = aetx_sign:hash(Tx),
    case aec_db:find_tx_location(Hash) of
        BlockHash when is_binary(BlockHash) ->
            lager:debug("Already have tx: ~p in ~p", [Hash, BlockHash]),
            {error, already_accepted};
        mempool ->
            %% lager:debug("Already have tx: ~p in ~p", [Hash, mempool]),
            ok;
        none ->
            Checks = [ fun check_signature/2
                     , fun check_nonce/2
                     , fun check_minimum_fee/2
                     ],
            case aeu_validation:run(Checks, [Tx, Hash]) of
                {error, _} = E ->
                    lager:debug("Validation error for tx ~p: ~p", [Hash, E]),
                    E;
                ok ->
                    case ets:member(Mempool, Key) of
                        true ->
                            lager:debug("Pool db key already present (~p)", [Key]),
                            %% TODO: We should make a decision whether to switch the tx.
                            ok;
                        false ->
                            lager:debug("Adding tx: ~p", [Hash]),
                            aec_db:add_tx(Tx),
                            ets:insert(Mempool, {Key, Tx}),
                            aec_events:publish(Event, Tx),
                            ok
                    end
            end
    end.

check_signature(Tx, Hash) ->
    {ok, Trees} = aec_chain:get_top_state(),
    case aetx_sign:verify(Tx, Trees) of
        {error, _} = E ->
            lager:info("Failed signature check on tx: ~p, ~p\n", [E, Hash]),
            E;
        ok ->
            ok
    end.

check_nonce(Tx,_Hash) ->
  check_nonce_at_hash(Tx, aec_chain:top_block_hash()).

check_nonce_at_hash(Tx, BlockHash) ->
    %% Check is conservative and only rejects certain cases
    Unsigned = aetx_sign:tx(Tx),
    TxNonce = aetx:nonce(Unsigned),
    case aetx:origin(Unsigned) of
        undefined -> {error, no_origin};
        Pubkey when is_binary(Pubkey) ->
            case TxNonce > 0 of
                false ->
                    {error, illegal_nonce};
                true ->
                    case aec_chain:get_account_at_hash(Pubkey, BlockHash) of
                        {error, no_state_trees} ->
                            ok;
                        none ->
                            ok;
                        {value, Account} ->
                            case aetx_utils:check_nonce(Account, TxNonce) of
                                ok -> ok;
                                {error, account_nonce_too_low} ->
                                    %% This can be ok in the future
                                    ok;
                                {error, account_nonce_too_high} = E ->
                                    E
                            end
                    end
            end
    end.

check_minimum_fee(Tx,_Hash) ->
    case aetx:fee(aetx_sign:tx(Tx)) >= aec_governance:minimum_tx_fee() of
        true  -> ok;
        false -> {error, too_low_fee}
    end.

sync_start(State, PeerId) ->
    {Pid, Ref} = spawn_monitor(fun() -> aec_tx_pool_sync(PeerId) end),
    State#state{ sync = {active, PeerId, {Pid, Ref}} }.

aec_tx_pool_sync(PeerId) ->
    case build_tx_mpt() of
        {ok, TxTree} ->
            lager:debug("ZZ tree built ~p", [PeerId]),
            aec_tx_pool_sync_loop(PeerId, TxTree, init);
        {error, Reason} ->
            lager:debug("ZZ tree failed", []),
            aec_tx_pool:sync_finish(PeerId, {error, Reason})
    end.

aec_tx_pool_sync_loop(PeerId, TxTree, init) ->
    case aec_peer_connection:tx_pool_sync_init(PeerId) of
        ok ->
            lager:debug("ZZ init ok ~p", [PeerId]),
            InitUnfolds = [{node, <<>>, aeu_mp_trees:root_hash(TxTree)}],
            aec_tx_pool_sync_loop(PeerId, TxTree, InitUnfolds, []);
        Err = {error, _} ->
            lager:debug("ZZ init error ~p", [Err]),
            aec_tx_pool:sync_abort(PeerId, Err)
    end.

aec_tx_pool_sync_loop(PeerId, _TxTree, [], []) ->
    aec_peer_connection:tx_pool_sync_abort(PeerId),
    aec_tx_pool:sync_finish(PeerId, done);
aec_tx_pool_sync_loop(PeerId, TxTree, [], Gets) ->
    lager:debug("ZZ getting ~p", [Gets]),
    case aec_peer_connection:tx_pool_sync_get(PeerId, Gets) of
        {ok, Txs} -> %% Should we verify the content?
            [ aec_tx_pool:push(Tx) || Tx <- Txs ],
            aec_tx_pool_sync_loop(PeerId, TxTree, [], []);
        Err = {error, _} ->
            aec_tx_pool:sync_abort(PeerId, Err)
    end;
aec_tx_pool_sync_loop(PeerId, TxTree, Unfolds, Gets) ->
    SerUnfolds = serialize_unfolds(Unfolds),
    lager:debug("ZZ unfolding ~p", [Unfolds]),
    case aec_peer_connection:tx_pool_sync_unfold(PeerId, SerUnfolds) of
        {ok, NewSerUnfolds} ->
            NewUnfolds = deserialize_unfolds(NewSerUnfolds),
            {NewUnfolds1, NewGets1} = analyze_unfolds(NewUnfolds, TxTree),
            lager:debug("ZZ new unfold ~p", [{NewUnfolds1, NewGets1}]),
            aec_tx_pool_sync_loop(PeerId, TxTree, NewUnfolds1, Gets ++ NewGets1);
        Err = {error, _} ->
            aec_tx_pool:sync_abort(PeerId, Err)
    end.

build_tx_mpt() ->
    try
        F = fun(TxHash, Tree) ->
                aeu_mp_trees:put(TxHash, [], Tree)
            end,
        Tree = aec_db:fold_mempool(F, aeu_mp_trees:new()),
        {ok, Tree}
    catch _:R ->
        {error, R}
    end.

serialize_unfolds(Us) ->
    [ serialize_unfold(U) || U <- Us ].

deserialize_unfolds(SUs) ->
    [ deserialize_unfold(SU) || SU <- SUs ].

serialize_unfold({node, Path, Node}) ->
    aeu_rlp:encode(
        aec_serialization:encode_fields(
            [{type, int}, {path, binary}, {node, binary}],
            [{type, 0}, {path, Path}, {node, Node}]));
serialize_unfold({key, Key}) ->
    aeu_rlp:encode(
        aec_serialization:encode_fields(
            [{type, int}, {key, binary}],
            [{type, 1}, {key, Key}]));
serialize_unfold({subtree, Path}) ->
    aeu_rlp:encode(
        aec_serialization:encode_fields(
            [{type, int}, {subtree, binary}],
            [{type, 2}, {subtree, Path}])).

deserialize_unfold(Blob) ->
    try
        [TypeBin | Fields] = aeu_rlp:decode(Blob),
        [{type, Type}] = aec_serialization:decode_fields([{type, int}], [TypeBin]),
        deserialize_unfold(Type, Fields)
    catch _:Reason ->
        {error, Reason}
    end.

deserialize_unfold(0, Flds) ->
    [{path, Path}, {node, Node}] =
        aec_serialization:decode_fields([{path, binary}, {node, binary}], Flds),
    {node, Path, Node};
deserialize_unfold(1, Flds) ->
    [{key, Key}] = aec_serialization:decode_fields([{key, binary}], Flds),
    {key, Key};
deserialize_unfold(2, Flds) ->
    [{subtree, Path}] = aec_serialization:decode_fields([{subtree, binary}], Flds),
    {subtree, Path}.


analyze_unfolds(Us, Tree) ->
    analyze_unfolds(Us, Tree, [], []).

analyze_unfolds([], _Tree, NewUs, NewGets) ->
    {lists:reverse(NewUs), lists:reverse(NewGets)};
analyze_unfolds([{key, Key} | Us], Tree, NewUs, NewGets) ->
    analyze_unfolds(Us, Tree, NewUs, [Key | NewGets]);
analyze_unfolds([N = {node, Path, Node} | Us], Tree, NewUs, NewGets) ->
    case aeu_mp_trees:has_node(Path, Node, Tree) of
        no ->
            analyze_unfolds(Us, Tree, [{subtree, Path} | NewUs], NewGets);
        partially ->
            analyze_unfolds(Us, Tree, [N | NewUs], NewGets);
        yes ->
            analyze_unfolds(Us, Tree, NewUs, NewGets)
    end.

-define(MAX_SYNCS, 3).

handle_sync_init(State = #state{ sync_out = Ss }, PeerId) ->
    lager:debug("ZZZ sync_init ~p", [PeerId]),
    case length(Ss) >= ?MAX_SYNCS of
        true ->
            {reply, {error, too_many_simultaneous_sync_activities}, State};
        false ->
            case lists:keyfind(PeerId, 2, Ss) of
                false ->
                    do_sync_init(State, PeerId);
                _ ->
                    {reply, {error, already_syncing_with_peer}, State}
            end
    end.

do_sync_init(State = #state{ sync_out = Ss }, PeerId) ->
    {ok, Pid} = aec_tx_pool_sync:start_link(),
    {reply, ok, State#state{ sync_out = [{sync, PeerId, Pid} | Ss] }}.

do_sync_finish(State = #state{ sync = {active, PeerId, {_, Ref}} }, PeerId, done) ->
    erlang:demonitor(Ref, [flush]),
    State#state{ sync = in_sync };
do_sync_finish(State = #state{ sync = {active, PeerId, {_, Ref}} }, PeerId, {error, _Reason}) ->
    erlang:demonitor(Ref, [flush]),
    State#state{ sync = none };
do_sync_finish(State, _PeerId, _Result) ->
    State.

do_sync_abort(State = #state{ sync = {active, PeerId, {_, Ref}}  }, PeerId) ->
    lager:debug("ZZ sync_abort ~p", [PeerId]),
    erlang:demonitor(Ref, [flush]),
    State#state{ sync = none };
do_sync_abort(State = #state{ sync_out = Ss }, PeerId) ->
    case lists:keyfind(PeerId, 2, Ss) of
        false ->
            State;
        {sync, PeerId, Pid} ->
            lager:debug("ZZ sync_abort ~p", [{PeerId, Pid}]),
            aec_tx_pool_sync:stop(Pid),
            State#state{ sync_out = lists:keydelete(PeerId, 2, Ss) }
    end.

