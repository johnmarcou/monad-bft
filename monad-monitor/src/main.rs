// Copyright (C) 2025 Category Labs, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use std::{
    collections::{BTreeMap, HashMap},
    io::ErrorKind,
    num::NonZero,
    path::{Path, PathBuf},
};

use alloy_primitives::U256;
use clap::{CommandFactory, FromArgMatches, Parser};
use futures_util::{Stream, StreamExt};
use inotify::{Inotify, WatchMask};
use lru::LruCache;
use monad_block_persist::{BlockPersist, FileBlockPersist, BLOCKDB_HEADERS_PATH};
use monad_consensus_types::{
    block::{ConsensusBlockHeader, ConsensusFullBlock},
    quorum_certificate::QuorumCertificate,
    timeout::HighExtend,
    validator_data::ValidatorsConfig,
    RoundCertificate,
};
use monad_crypto::certificate_signature::PubKey;
use monad_node_config::{
    ExecutionProtocolType, ForkpointConfig, NodeBootstrapConfig, SignatureCollectionType,
    SignatureType,
};
use monad_types::{BlockId, Epoch, NodeId, Round, Stake};
use monad_validator::{
    leader_election::LeaderElection, signature_collection::SignatureCollection,
    weighted_round_robin::WeightedRoundRobin,
};
use tracing::{error, info, warn};
use tracing_subscriber::{
    fmt::{format::FmtSpan, Layer},
    layer::SubscriberExt,
};

const MAX_REWIND_QUEUE_LEN: usize = 100;

type BlockHeader =
    ConsensusBlockHeader<SignatureType, SignatureCollectionType, ExecutionProtocolType>;

struct CachedBlock {
    header: BlockHeader,
    is_finalized: bool,
}
impl CachedBlock {
    fn new(header: BlockHeader) -> Self {
        Self {
            header,
            is_finalized: false,
        }
    }
}
type CachedBlocks = LruCache<BlockId, CachedBlock>;

type Pubkey = <SignatureCollectionType as SignatureCollection>::NodeIdPubKey;
type EpochStakes = BTreeMap<NodeId<Pubkey>, Stake>;

fn load_epoch_stakes(validators_path: &Path, epoch: Epoch) -> EpochStakes {
    let validators: ValidatorsConfig<SignatureCollectionType> =
        ValidatorsConfig::read_from_path(validators_path).unwrap_or_else(|err| {
            panic!(
                "failed to read validators.toml, or validators.toml corrupt. was this edited manually? err={:?}",
                err
            )
        });
    validators
        .get_validator_set(&epoch)
        .expect("validator set should exist for current epoch")
        .get_stakes()
        .into_iter()
        .collect()
}

/// basis points (0-10_000) of `part` relative to `total`; 0 if total is zero
fn stake_bps(part: U256, total: U256) -> u64 {
    if total == U256::ZERO {
        return 0;
    }
    (part * U256::from(10_000) / total).to::<u64>()
}

fn finalize_block(
    blocks: &mut CachedBlocks,
    finalized_block_id: &BlockId,
    finalize_fn: impl Fn(&BlockHeader),
) {
    let mut finalized_block_id = *finalized_block_id;
    let mut to_finalize = Vec::new();
    while let Some(finalized_block) = blocks.peek_mut(&finalized_block_id) {
        if finalized_block.is_finalized {
            // already finalized
            break;
        }
        finalized_block.is_finalized = true;
        let finalized_block_header = &finalized_block.header;
        to_finalize.push(finalized_block_header.clone());
        // try finalizing parent
        finalized_block_id = finalized_block_header.get_parent_id();
    }

    // finalize in reverse order (oldest first)
    for finalized_block_header in to_finalize.iter().rev() {
        finalize_fn(finalized_block_header)
    }
}

#[tokio::main]
async fn main() {
    let subscriber = tracing_subscriber::Registry::default().with(
        Layer::default()
            .json()
            .with_span_events(FmtSpan::NONE)
            .with_current_span(false)
            .with_span_list(false)
            .with_writer(std::io::stdout)
            .with_ansi(false),
    );
    tracing::subscriber::set_global_default(subscriber).expect("unable to set default subscriber");

    let mut cmd = Cli::command();
    let Cli {
        forkpoint_path,
        ledger_path,
        peers_path,
        validators_path,
        self_pubkey,
    } = Cli::from_arg_matches_mut(&mut cmd.get_matches_mut()).expect("unable to parse CLI");

    let self_node_id: Option<NodeId<Pubkey>> = self_pubkey.map(|hex_str| {
        let bytes = hex::decode(hex_str.trim_start_matches("0x"))
            .expect("--self-pubkey must be valid hex");
        let pubkey =
            Pubkey::from_bytes(&bytes).unwrap_or_else(|err| panic!("invalid --self-pubkey: {:?}", err));
        NodeId::new(pubkey)
    });

    let mut visited_blocks: CachedBlocks = LruCache::new(NonZero::new(100).unwrap());

    let addresses: HashMap<_, _> = std::fs::read_to_string(&peers_path)
        .ok()
        .and_then(|s| toml::from_str::<NodeBootstrapConfig<SignatureType>>(&s).ok())
        .map(|config| {
            config
                .peers
                .into_iter()
                .map(|peer| {
                    (
                        peer.secp256k1_pubkey,
                        format!(
                            "{}:{}",
                            peer.ip()
                                .expect("ledger tail bootstrap peer address must be an IP address"),
                            peer.tcp_port()
                        ),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let get_author_address =
        |pubkey: &_| -> String { addresses.get(pubkey).cloned().unwrap_or_default() };

    let mut epoch_validators = BTreeMap::default();

    let block_persist: FileBlockPersist<
        SignatureType,
        SignatureCollectionType,
        ExecutionProtocolType,
    > = FileBlockPersist::new(ledger_path.clone());

    // stake-weighted liveness tracking: last time each validator was the leader
    // skipped by a timeout, and which epoch to report on every REPORT_INTERVAL tick.
    let mut last_skipped: BTreeMap<NodeId<Pubkey>, std::time::Duration> = BTreeMap::new();
    let mut current_epoch = Epoch(1);
    const REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);
    const LIVENESS_WINDOW: std::time::Duration = std::time::Duration::from_secs(10 * 60);
    let mut report_interval = tokio::time::interval(REPORT_INTERVAL);
    report_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut last_high_certificate = RoundCertificate::Qc(QuorumCertificate::genesis_qc());
    let mut tip_stream = Box::pin(latest_tip_stream(&forkpoint_path, &ledger_path));
    loop {
        tokio::select! {
            maybe_tip = tip_stream.next() => {
                let Some((high_certificate, proposed_head)) = maybe_tip else {
                    break;
                };
                let now_ts = std::time::UNIX_EPOCH.elapsed().unwrap();

                if last_high_certificate != high_certificate {
                    match &high_certificate {
                        RoundCertificate::Qc(qc) => {
                            current_epoch = qc.get_epoch();
                            if !epoch_validators.contains_key(&current_epoch) {
                                let stakes = load_epoch_stakes(&validators_path, current_epoch);
                                info!(
                                    epoch =? current_epoch.0,
                                    validators =? stakes,
                                    "validator_set"
                                );
                                epoch_validators.insert(current_epoch, stakes);
                            }

                            info!(
                                round =? qc.get_round().0,
                                epoch =? qc.get_epoch().0,
                                block_id =? qc.get_block_id(),
                                num_signatures =? qc.signatures.num_signatures(),
                                now_ts_ms =? now_ts.as_millis(),
                                "quorum_certificate"
                            );
                        }
                        RoundCertificate::Tc(tc) => {
                            current_epoch = tc.epoch;
                            let validators = if let Some(validators) = epoch_validators.get(&tc.epoch) {
                                validators
                            } else {
                                let stakes = load_epoch_stakes(&validators_path, tc.epoch);
                                info!(
                                    epoch =? tc.epoch.0,
                                    validators =? stakes,
                                    "validator_set"
                                );
                                epoch_validators.entry(tc.epoch).or_insert(stakes)
                            };

                            // per distinct (high_qc_round, high_tip_round) bucket reported by timed-out nodes
                            let tip_round_buckets: Vec<(u64, u64, usize)> = tc
                                .tip_rounds
                                .iter()
                                .map(|bucket| {
                                    (
                                        bucket.high_qc_round.0,
                                        bucket.high_tip_round.0,
                                        bucket.sigs.num_signatures(),
                                    )
                                })
                                .collect();
                            let num_timeout_signers: usize =
                                tip_round_buckets.iter().map(|(_, _, n)| n).sum();

                            let skipped_round = tc.round;
                            let skipped_leader =
                                WeightedRoundRobin::default().get_leader(skipped_round, validators);
                            // this validator was leader for a round that timed out instead of
                            // producing a proposal -> counts as offline for LIVENESS_WINDOW
                            last_skipped.insert(skipped_leader.clone(), now_ts);

                            let high_extend_qc = tc.high_extend.qc();
                            let high_extend_kind = match &tc.high_extend {
                                HighExtend::Qc(_) => "qc",
                                HighExtend::Tip(_) => "tip",
                            };

                            info!(
                                round =? skipped_round.0,
                                epoch =? tc.epoch.0,
                                author =? skipped_leader,
                                author_address = %get_author_address(&skipped_leader.pubkey()),
                                num_timeout_signers =? num_timeout_signers,
                                tip_round_buckets =? tip_round_buckets,
                                high_extend_kind,
                                high_extend_qc_round =? high_extend_qc.get_round().0,
                                high_extend_qc_epoch =? high_extend_qc.get_epoch().0,
                                high_extend_qc_block_id =? high_extend_qc.get_block_id(),
                                now_ts_ms =? now_ts.as_millis(),
                                "timeout"
                            );
                        }
                    }
                }

                let mut block_queue = Vec::new();
                let mut next_block_id = if high_certificate.qc().get_round() >= proposed_head.block_round {
                    high_certificate.qc().get_block_id()
                } else {
                    proposed_head.get_id()
                };
                loop {
                    if visited_blocks.contains(&next_block_id) || block_queue.len() > MAX_REWIND_QUEUE_LEN {
                        break;
                    }
                    if let Ok(next_block_header) = block_persist.read_bft_header(&next_block_id) {
                        next_block_id = next_block_header.get_parent_id();
                        block_queue.push(next_block_header);
                    } else {
                        break;
                    }
                }

                for block_header in block_queue.into_iter().rev() {
                    let Ok(block_body) = block_persist.read_bft_body(&block_header.block_body_id) else {
                        // no body for block header, so skip and move on
                        continue;
                    };
                    let block = ConsensusFullBlock::new(block_header, block_body).expect("block is valid");

                    visited_blocks.put(block.get_id(), CachedBlock::new(block.header().clone()));

                    info!(
                        round =? block.get_block_round().0,
                        parent_round =? block.get_qc().get_round().0,
                        epoch =? block.header().epoch.0,
                        seq_num =? block.header().seq_num.0,
                        num_tx =? block.body().execution_body.transactions.len(),
                        author =? block.header().author,
                        block_ts_ms =? block.header().timestamp_ns / 1_000_000,
                        now_ts_ms =? now_ts.as_millis(),
                        author_address = %get_author_address(&block.header().author.pubkey()),
                        "proposed_block"
                    );
                }

                if last_high_certificate != high_certificate {
                    if let RoundCertificate::Qc(qc) = &high_certificate {
                        if let Some(parent_block) = visited_blocks.peek(&qc.get_block_id()) {
                            let parent_qc = parent_block.header.qc.clone();
                            if qc.get_round() == parent_qc.get_round() + Round(1) {
                                // commit rule passed
                                finalize_block(
                                    &mut visited_blocks,
                                    &parent_qc.get_block_id(),
                                    |finalized_block_header| {
                                        info!(
                                            round =? finalized_block_header.block_round.0,
                                            parent_round =? finalized_block_header.qc.get_round().0,
                                            epoch =? finalized_block_header.epoch.0,
                                            seq_num =? finalized_block_header.seq_num.0,
                                            author =? finalized_block_header.author,
                                            block_ts_ms =? finalized_block_header.timestamp_ns / 1_000_000,
                                            now_ts_ms =? now_ts.as_millis(),
                                            author_address = %get_author_address(&finalized_block_header.author.pubkey()),
                                            "finalized_block"
                                        )
                                    },
                                );
                            }
                        }
                    }
                }

                last_high_certificate = high_certificate.clone();
                while epoch_validators.len() > 1_000 {
                    epoch_validators.pop_first();
                }
            }

            _ = report_interval.tick() => {
                let Some(stakes) = epoch_validators.get(&current_epoch) else {
                    continue;
                };
                let now_ts = std::time::UNIX_EPOCH.elapsed().unwrap();

                // offline = this validator was the leader skipped by a timeout within
                // LIVENESS_WINDOW; online is everyone else (default assumption)
                let is_offline = |node_id: &NodeId<Pubkey>| {
                    last_skipped
                        .get(node_id)
                        .is_some_and(|skipped| now_ts.saturating_sub(*skipped) <= LIVENESS_WINDOW)
                };

                let mut total_stake = U256::ZERO;
                let mut offline_stake = U256::ZERO;
                for (node_id, stake) in stakes.iter() {
                    total_stake += stake.0;
                    if is_offline(node_id) {
                        offline_stake += stake.0;
                    }
                }
                let online_stake = total_stake - offline_stake;
                let online_pct = stake_bps(online_stake, total_stake) as f64 / 100.0;

                let self_stake = match &self_node_id {
                    Some(id) => stakes.get(id).map(|s| s.0),
                    None => None,
                };
                let self_offline = self_node_id.as_ref().is_some_and(is_offline);

                // stake that goes offline if this validator turns off, and the
                // resulting online stake/percentage estimate
                let turn_off_stake = self_stake.unwrap_or(U256::ZERO);
                let online_stake_after_turn_off = if self_offline {
                    online_stake
                } else {
                    online_stake - turn_off_stake
                };
                let online_pct_after_turn_off =
                    stake_bps(online_stake_after_turn_off, total_stake) as f64 / 100.0;

                info!(
                    epoch =? current_epoch.0,
                    total_stake = %total_stake,
                    online_stake = %online_stake,
                    offline_stake = %offline_stake,
                    online_pct,
                    self_stake =? self_stake.map(|s| s.to_string()),
                    turn_off_stake =? self_stake.map(|s| s.to_string()),
                    online_pct_after_turn_off,
                    now_ts_ms =? now_ts.as_millis(),
                    "stake_liveness"
                );
            }
        }
    }
}

pub fn latest_tip_stream(
    forkpoint_path: &Path,
    ledger_path: &Path,
) -> impl Stream<
    Item = (
        RoundCertificate<SignatureType, SignatureCollectionType, ExecutionProtocolType>,
        BlockHeader,
    ),
> {
    let inotify = Inotify::init().expect("error initializing inotify");
    inotify
        .watches()
        .add(
            {
                let mut headers_path = PathBuf::from(ledger_path);
                headers_path.push(BLOCKDB_HEADERS_PATH);
                headers_path
            },
            WatchMask::CLOSE_WRITE,
        )
        .expect("failed to watch ledger path");
    inotify
        .watches()
        .add(
            {
                let mut forkpoint_dir = PathBuf::from(forkpoint_path);
                forkpoint_dir.pop();
                forkpoint_dir
            },
            WatchMask::CLOSE_WRITE | WatchMask::MOVE,
        )
        .expect("failed to watch forkpoint path");

    let inotify_buffer = [0; 1024];
    let inotify_events = inotify
        .into_event_stream(inotify_buffer)
        .expect("failed to create inotify event stream");

    let block_persist: FileBlockPersist<
        SignatureType,
        SignatureCollectionType,
        ExecutionProtocolType,
    > = FileBlockPersist::new(ledger_path.to_owned());

    let forkpoint_path = forkpoint_path.to_owned();

    inotify_events.filter_map(move |maybe_event| {
        let result = (|| match maybe_event {
            Ok(_event) => {
                let forkpoint_config = ForkpointConfig::try_parse_bytes(
                    &forkpoint_path.to_string_lossy(),
                    &std::fs::read(&forkpoint_path).ok()?,
                )
                .ok()?;
                let proposed_head = block_persist.read_proposed_head_bft_header().ok()?;
                Some((forkpoint_config.high_certificate, proposed_head))
            }
            Err(err) if err.kind() == ErrorKind::InvalidInput => {
                warn!(
                    ?err,
                    "ErrorKind::InvalidInput, are files being produced faster than indexer?"
                );
                None
            }
            Err(err) => {
                error!(?err, "inotify error while reading events");
                panic!("inotify error while reading events")
            }
        })();
        async move { result }
    })
}

#[derive(Debug, Parser)]
#[command(about, long_about = None)]
pub struct Cli {
    #[arg(long, default_value = "/monad/ledger")]
    pub ledger_path: PathBuf,

    #[arg(long, default_value = "/monad/config/forkpoint/forkpoint.toml")]
    pub forkpoint_path: PathBuf,

    #[arg(long, default_value = "/monad/config/peers.toml")]
    pub peers_path: PathBuf,

    #[arg(long, default_value = "/monad/config/validators/validators.toml")]
    pub validators_path: PathBuf,

    /// hex-encoded compressed secp256k1 pubkey identifying this node's own
    /// validator (same format as `secp256k1_pubkey` in peers.toml) — used to
    /// report the stake that would go offline if this validator turned off
    #[arg(long)]
    pub self_pubkey: Option<String>,
}
