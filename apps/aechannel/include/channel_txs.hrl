%%%=============================================================================
%%% @copyright (C) 2018, Aeternity Anstalt
%%% @doc
%%%    Records for State Channels transactions
%%% @end
%%%=============================================================================
-type vsn() :: non_neg_integer().

-record(channel_create_tx, {
          initiator          :: aec_keys:pubkey(),
          initiator_amount   :: non_neg_integer(),
          responder          :: aec_keys:pubkey(),
          responder_amount   :: non_neg_integer(),
          channel_reserve    :: non_neg_integer(),
          lock_period        :: non_neg_integer(),
          ttl                :: aetx:tx_ttl(),
          fee                :: non_neg_integer(),
          state_hash         :: binary(),
          nonce              :: non_neg_integer()
         }).

-record(channel_deposit_tx, {
          channel_id  :: binary(),
          from        :: aec_keys:pubkey(),
          amount      :: non_neg_integer(),
          ttl         :: aetx:tx_ttl(),
          fee         :: non_neg_integer(),
          state_hash  :: binary(),
          round       :: integer(),
          nonce       :: non_neg_integer()
         }).

-record(channel_withdraw_tx, {
          channel_id  :: binary(),
          to          :: aec_keys:pubkey(),
          amount      :: non_neg_integer(),
          ttl         :: aetx:tx_ttl(),
          fee         :: non_neg_integer(),
          state_hash  :: binary(),
          round       :: integer(),
          nonce       :: non_neg_integer()
         }).

-record(channel_close_mutual_tx, {
          channel_id        :: binary(),
          initiator_amount  :: non_neg_integer(),
          responder_amount  :: non_neg_integer(),
          ttl               :: aetx:tx_ttl(),
          fee               :: non_neg_integer(),
          state_hash        :: binary(),
          round             :: integer(),
          nonce             :: non_neg_integer()
         }).

-record(channel_close_solo_tx, {
          channel_id :: binary(),
          from       :: aec_keys:pubkey(),
          payload    :: binary(),
          poi        :: aec_trees:poi(),
          ttl        :: aetx:tx_ttl(),
          fee        :: non_neg_integer(),
          nonce      :: non_neg_integer()
         }).

-record(channel_slash_tx, {
          channel_id :: binary(),
          from       :: aec_keys:pubkey(),
          payload    :: binary(),
          poi        :: aec_trees:poi(),
          ttl        :: aetx:tx_ttl(),
          fee        :: non_neg_integer(),
          nonce      :: non_neg_integer()
         }).

-record(channel_settle_tx, {
          channel_id        :: binary(),
          from              :: aec_keys:pubkey(),
          initiator_amount  :: non_neg_integer(),
          responder_amount  :: non_neg_integer(),
          ttl               :: aetx:tx_ttl(),
          fee               :: non_neg_integer(),
          state_hash        :: binary(),
          round             :: integer(),
          nonce             :: non_neg_integer()
         }).

-record(channel_offchain_tx, {
          channel_id         :: binary(),
          initiator          :: aec_keys:pubkey(),
          responder          :: aec_keys:pubkey(),
          initiator_amount   :: aesc_channels:amount(),
          responder_amount   :: aesc_channels:amount(),
          updates            :: [aesc_offchain_state:update()],
          state_hash         :: binary(),
          previous_round     :: non_neg_integer(),
          round              :: non_neg_integer()
         }).

