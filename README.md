# MrRewards

OpenSource GRPC rust client (Keeper) handling Liquidation, SL and TP for the adrena program.

See MrSablierStaking for the staking related counterpart.

## Build

`$> cargo build`
`$> cargo build --release`

## Run

`$> cargo run -- --payer-keypair payer.json --endpoint https://adrena.rpcpool.com/xxx --commitment finalized`

Ideally run that on a Render instance.