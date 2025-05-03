use { 
    anchor_client::{solana_sdk::signer::keypair::read_keypair_file, Client, Cluster}, anchor_spl::token::spl_token, backoff::{future::retry, ExponentialBackoff}, clap::Parser, futures::TryFutureExt, openssl::ssl::{SslConnector, SslMethod}, postgres_openssl::MakeTlsConnector, priority_fees::fetch_mean_priority_fee, solana_sdk::{compute_budget::ComputeBudgetInstruction, pubkey::Pubkey, signer::Signer, transaction::Transaction}, spl_associated_token_account::{get_associated_token_address, instruction::create_associated_token_account_idempotent}, std::{env, str::FromStr, sync::Arc, time::Duration}, tokio::{
        sync::Mutex,
        task::JoinHandle,
        time::interval,
    }, yellowstone_grpc_proto::prelude::CommitmentLevel
};
use solana_sdk::signature::Keypair;
use anchor_client::Program;

pub mod priority_fees;
pub mod utils;

const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:10000";
const MEAN_PRIORITY_FEE_PERCENTILE: u64 = 5000; // 50th
const PRIORITY_FEE_REFRESH_INTERVAL: Duration = Duration::from_secs(5); // seconds
const ADX_MINT: &str = "AuQaustGiaqxRvj2gtCdrd22PBzTn8kM3kEPEkZCtuDw";
const JTO_MINT: &str = "jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL";
const BONK_MINT: &str = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263";
const ADX_DECIMALS: u8 = 6;
const JTO_DECIMALS: u8 = 9;
const BONK_DECIMALS: u8 = 5;

#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
enum ArgsCommitment {
    #[default]
    Processed,
    Confirmed,
    Finalized,
}

impl From<ArgsCommitment> for CommitmentLevel {
    fn from(commitment: ArgsCommitment) -> Self {
        match commitment {
            ArgsCommitment::Processed => CommitmentLevel::Processed,
            ArgsCommitment::Confirmed => CommitmentLevel::Confirmed,
            ArgsCommitment::Finalized => CommitmentLevel::Finalized,
        }
    }
}

#[derive(Debug, Clone, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from(DEFAULT_ENDPOINT))]
    /// Service endpoint
    endpoint: String,

    #[clap(long)]
    x_token: Option<String>,

    /// Commitment level: processed, confirmed or finalized
    #[clap(long)]
    commitment: Option<ArgsCommitment>,

    /// Path to the payer keypair
    #[clap(long)]
    payer_keypair: String,

    /// DB Url
    #[clap(long)]
    db_string: String,

    /// Combined certificate
    #[clap(long)]
    combined_cert: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();
    let zero_attempts = Arc::new(Mutex::new(true));

    // The default exponential backoff strategy intervals:
    // [500ms, 750ms, 1.125s, 1.6875s, 2.53125s, 3.796875s, 5.6953125s,
    // 8.5s, 12.8s, 19.2s, 28.8s, 43.2s, 64.8s, 97s, ... ]
    retry(ExponentialBackoff::default(), move || {
        let args = args.clone();
        let zero_attempts = Arc::clone(&zero_attempts);
        let mut periodical_priority_fees_fetching_task: Option<JoinHandle<Result<(), backoff::Error<anyhow::Error>>>> = None;       
        let mut db_connection_task: Option<JoinHandle<()>> = None;

        async move {
            // In case it errored out, abort the fee task (will be recreated)
            if let Some(t) = periodical_priority_fees_fetching_task.take() {
                t.abort();
            }
            if let Some(t) = db_connection_task.take() {
                t.abort();
            }


            // ////////////////////////////////////////////////////////////////
            //  Setup Program object for web3 queries
            // ////////////////////////////////////////////////////////////////
            let mut zero_attempts = zero_attempts.lock().await;
            if *zero_attempts {
                *zero_attempts = false;
            } else {
                log::info!("Retry to connect to the server");
            }
            drop(zero_attempts);

            let payer = read_keypair_file(args.payer_keypair.clone()).unwrap();
            let payer = Arc::new(payer);
            let client = Client::new(
                Cluster::Custom(args.endpoint.clone(), args.endpoint.clone()),
                Arc::clone(&payer),
            );
            let adrena_program = client
                .program(adrena_abi::ID)
                .map_err(|e| backoff::Error::transient(e.into()))?;
            log::info!("  <> gRPC, RPC clients connected!");

            
            // ////////////////////////////////////////////////////////////////
            // DB CONNECTION
            // ////////////////////////////////////////////////////////////////
            // Connect to the DB that contains the table matching the UserStaking accounts to their owners
            let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();

            // Use the combined certificate
            builder.set_ca_file(&args.combined_cert)
                .map_err(|e| {
                    log::error!("Failed to set CA file: {}", e);
                    backoff::Error::transient(anyhow::anyhow!("Failed to set CA file: {}", e))
                })?;

            let connector = MakeTlsConnector::new(builder.build());

            log::info!("Attempting PostgreSQL connection...");
            let (db, db_connection) = tokio_postgres::connect(&args.db_string, connector)
                .await
                .map_err(|e| {
                    log::error!("PostgreSQL connection error: {:?}", e);
                    backoff::Error::transient(e.into())
                })?;
            log::info!("PostgreSQL connection successful!");

            // Open a connection to the DB
            #[allow(unused_assignments)]
            {
                db_connection_task = Some(tokio::spawn(async move {
                    if let Err(e) = db_connection.await {
                        log::error!("PostgreSQL connection task error: {}", e);
                        log::error!("connection error: {}", e);
                    }
                }));
            }

            // ////////////////////////////////////////////////////////////////
            // Side thread to fetch the median priority fee every 5 seconds
            // ////////////////////////////////////////////////////////////////
            let median_priority_fee = Arc::new(Mutex::new(0u64));
            // Spawn a task to poll priority fees every 5 seconds
            log::info!("3 - Spawn a task to poll priority fees every 5 seconds...");
            #[allow(unused_assignments)]
            {
            periodical_priority_fees_fetching_task = Some({
                let median_priority_fee = Arc::clone(&median_priority_fee);
                tokio::spawn(async move {
                    let mut fee_refresh_interval = interval(PRIORITY_FEE_REFRESH_INTERVAL);
                    loop {
                        fee_refresh_interval.tick().await;
                        if let Ok(fee) =
                            fetch_mean_priority_fee(&client, MEAN_PRIORITY_FEE_PERCENTILE).await
                        {
                            let mut fee_lock = median_priority_fee.lock().await;
                            *fee_lock = fee;
                            log::debug!(
                                "  <> Updated median priority fee 30th percentile to : {} µLamports / cu",
                                fee
                            );
                        }
                    }
                    })
                });
            }

            // ////////////////////////////////////////////////////////////////
            // CORE LOOP
            //
            // - Get entry of reward to be distributed from DB (distributed == false && in_processing == false)
            // - write entry as "in_processing" == true
            // - send tx to distribute reward
            // - verify tx is successful
            // - write entry as "distributed" == true
            // - write entry as "in_processing" == false
            // ////////////////////////////////////////////////////////////////
            loop {
                let reward_entry = get_unprocessed_reward_entry_from_db(&db).await?;
                if let Some(reward_entry) = reward_entry {
                    distribute_reward(
                        &adrena_program,
                        &db,
                        &payer,
                        &reward_entry,
                        &median_priority_fee,
                    ).await?;
                } else {
                    log::info!("No unprocessed reward entry found in DB - Sleeping 5 seconds");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }


            Ok::<(), backoff::Error<anyhow::Error>>(())
        }
        .inspect_err(|error| log::error!("failed to connect: {error}"))
    })
    .await
    .map_err(Into::into)
}

async fn distribute_reward(
    adrena_program: &Program<Arc<Keypair>>,
    db: &tokio_postgres::Client,
    payer: &Arc<Keypair>,
    reward_entry: &RewardEntry,
    median_priority_fee: &Arc<Mutex<u64>>,
) -> Result<(), backoff::Error<anyhow::Error>> {
    let adx_native_amount = (reward_entry.adx_amount * 10_f64.powf(ADX_DECIMALS as f64)) as u64; // Convert to ADX (6 decimals)
    let jto_native_amount = (reward_entry.jto_amount * 10_f64.powf(JTO_DECIMALS as f64)) as u64; // Convert to JTO (9 decimals)
    let bonk_native_amount = (reward_entry.bonk_amount * 10_f64.powf(BONK_DECIMALS as f64)) as u64; // Convert to BONK (5 decimals)

    let adx_mint = Pubkey::from_str(ADX_MINT).unwrap();
    let jto_mint = Pubkey::from_str(JTO_MINT).unwrap();
    let bonk_mint = Pubkey::from_str(BONK_MINT).unwrap();

    let recipient = &reward_entry.recipient_pubkey;
                
    log::info!(" [reward ID {}] Start reward send processing to recipient {} (ADX {} | JTO {} | BONK {})", reward_entry.reward_id, recipient, adx_native_amount, jto_native_amount, bonk_native_amount);

    // update DB to set has_processing_started to true
    db.execute(
        "UPDATE rewards SET has_processing_started = true WHERE reward_id = $1",
        &[&(reward_entry.reward_id as i32)],
    ).await.map_err(|e| backoff::Error::transient(e.into()))?;

    let mut instructions = vec![];

    // Add compute budget instructions
    instructions.push(ComputeBudgetInstruction::set_compute_unit_price(*median_priority_fee.lock().await));
    instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(50_000));

    // Add token transfer instructions
    if adx_native_amount > 0 {
        let source_ata = get_associated_token_address(&payer.pubkey(), &adx_mint);
        let destination_ata = get_associated_token_address(&recipient, &adx_mint);
        
        instructions.push(create_associated_token_account_idempotent(
            &payer.pubkey(),
            &recipient,
            &adx_mint,
            &spl_token::ID,
        ));
        
        instructions.push(spl_token::instruction::transfer(
            &spl_token::ID,
            &source_ata,
            &destination_ata,
            &payer.pubkey(),
            &[],
            adx_native_amount,
        ).unwrap());
    }

    if jto_native_amount > 0 {
        let source_ata = get_associated_token_address(&payer.pubkey(), &jto_mint);
        let destination_ata = get_associated_token_address(&recipient, &jto_mint);
        
        instructions.push(create_associated_token_account_idempotent(
            &payer.pubkey(),
            &recipient,
            &jto_mint,
            &spl_token::ID,
        ));
        
        instructions.push(spl_token::instruction::transfer(
            &spl_token::ID,
            &source_ata,
            &destination_ata,
            &payer.pubkey(),
            &[],
            jto_native_amount,
        ).unwrap());
    }

    if bonk_native_amount > 0 {
        let source_ata = get_associated_token_address(&payer.pubkey(), &bonk_mint);
        let destination_ata = get_associated_token_address(&recipient, &bonk_mint);
        
        instructions.push(create_associated_token_account_idempotent(
            &payer.pubkey(),
            &recipient,
            &bonk_mint,
            &spl_token::ID,
        ));
        
        instructions.push(spl_token::instruction::transfer(
            &spl_token::ID,
            &source_ata,
            &destination_ata,
            &payer.pubkey(),
            &[],
            bonk_native_amount,
        ).unwrap());
    }

    let recent_blockhash = adrena_program.rpc().get_latest_blockhash().await.unwrap();
    let tx = Transaction::new_signed_with_payer(
        &instructions,
        Some(&payer.pubkey()),
        &[&payer],
        recent_blockhash,
    );

    let sig = adrena_program.rpc().send_and_confirm_transaction(&tx).await.unwrap();
    log::info!("  [DONE] Rewards sent successfully to recipient {}: {}", recipient, sig);
    
    // update DB to set is_processed to true
    db.execute(
        "UPDATE rewards SET is_processed = true WHERE reward_id = $1",
        &[&(reward_entry.reward_id as i32)],
    ).await.map_err(|e| backoff::Error::transient(e.into()))?;

    Ok(())
}

// Table "public.rewards"
// ┌────────────────────────┬──────────────────────────┬───────────┬──────────┬────────────────────────────────────────────┐
// │         Column         │           Type           │ Collation │ Nullable │                  Default                   │
// ├────────────────────────┼──────────────────────────┼───────────┼──────────┼────────────────────────────────────────────┤
// │ reward_id              │ integer                  │           │ not null │ nextval('rewards_reward_id_seq'::regclass) │
// │ user_pubkey            │ character varying(44)    │           │ not null │                                            │
// │ adx                    │ numeric(18,6)            │           │          │ 0                                          │
// │ bonk                   │ numeric(18,6)            │           │          │ 0                                          │
// │ jto                    │ numeric(18,6)            │           │          │ 0                                          │
// │ is_processed           │ boolean                  │           │          │ false                                      │
// │ reason                 │ text                     │           │          │ ''::text                                   │
// │ created_at             │ timestamp with time zone │           │          │ CURRENT_TIMESTAMP                          │
// │ updated_at             │ timestamp with time zone │           │          │ CURRENT_TIMESTAMP                          │
// │ has_processing_started │ boolean                  │           │          │ false                                      │
// └────────────────────────┴──────────────────────────┴───────────┴──────────┴────────────────────────────────────────────┘

pub struct RewardEntry {
    pub reward_id: u32,
    pub recipient_pubkey: Pubkey,
    pub adx_amount: f64,
    pub bonk_amount: f64,
    pub jto_amount: f64,
    pub is_processed: bool,
    pub has_processing_started: bool,
}

async fn get_unprocessed_reward_entry_from_db(
    db: &tokio_postgres::Client,
) -> Result<Option<RewardEntry>, backoff::Error<anyhow::Error>> {
    let rows = db
        .query(
            "SELECT reward_id, user_pubkey, adx::float8, bonk::float8, jto::float8, is_processed, has_processing_started FROM rewards WHERE is_processed = false AND has_processing_started = false",
            &[],
        )
        .await
        .map_err(|e| backoff::Error::transient(e.into()))?;

    if let Some(row) = rows.first() {
        Ok(Some(
            RewardEntry {
                reward_id: row.get::<_, i32>(0) as u32,
                recipient_pubkey: Pubkey::from_str(row.get::<_, String>(1).as_str()).expect("Invalid pubkey"),
                adx_amount: row.get::<_, f64>(2),
                bonk_amount: row.get::<_, f64>(3),
                jto_amount: row.get::<_, f64>(4),
                is_processed: row.get::<_, bool>(5),
                has_processing_started: row.get::<_, bool>(6),
            }
        ))
    } else {
        log::debug!(
            "No unprocessed reward entry found in DB"
        );
        Ok(None)
    }
}