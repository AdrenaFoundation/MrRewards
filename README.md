# MrRewards

OpenSource rust client (Keeper) handling reward distribution from the Adrena database.

## Build

`$> cargo build`
`$> cargo build --release`

## Run

`$> cargo run -- --payer-keypair payer.json --endpoint https://adrena.rpcpool.com/xxx --commitment finalized`

Ideally run that on a Render instance.