# MrSablier

OpenSource GRPC rust client (Keeper) handling Liquidation, SL and TP for the adrena program.

See MrSablierStaking for the staking related counterpart.

## Build

`$> cargo build`
`$> cargo build --release`

## Run

`$> cargo run -- --payer-keypair payer.json --endpoint https://adrena.rpcpool.com/xxx --commitment finalized`

Ideally run that on a Render instance. Else Daemon setup available below

### Run as a service using [Daemon](https://www.libslack.org/daemon/manual/daemon.1.html)

`daemon --name=mrsablier --output=/home/ubuntu/MrSablier/logfile.log -- /home/ubuntu/MrSablier/target/release/mrsablier --payer-keypair /home/ubuntu/MrSablier/mr_sablier.json --endpoint https://adrena-solanam-6f0c.mainnet.rpcpool.com/<> --x-token <> --commitment processed`

### Monitor Daemon logs

`tail -f -n 100 ~/MrSablier/logfile.log | tspin`

### Stop Daemon

`daemon --name=mrsablier --stop`