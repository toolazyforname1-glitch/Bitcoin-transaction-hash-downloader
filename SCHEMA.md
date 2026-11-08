# A_txid_collector.py — file schemas

## Parquet output (`data/<start>-<end>.parquet`, snappy)

Explicit schema (`TXID_TABLE_SCHEMA` in code):

| column | type | meaning |
|---|---|---|
| `marker` | int64 | block height on a marker row, else the 1-based tx counter within its block |
| `txid` | string | `""` on a marker row, else the txid hex string |

Rows are strict block/tx order. Files are capped at `--file-cap-bytes`
(default 1 GiB); the block that would cross the cap rolls into the next file.

## Merkle-root output (`data/roots.csv`, `--root-only`)

One line per block: `<height>,<merkle_root_hex>`. About 72 bytes per block, so
the whole chain is ~70 MB instead of ~90 GB.

The root is computed from that block's txids with a streaming ladder: each txid
is double-SHA256'd into a level-0 node, two nodes at the same level merge into
one at the next, and an unpaired node waits until a sibling arrives. At the end
of the block the leftover stack is folded, duplicating each odd trailing node
exactly as Bitcoin Core does, so the result is byte-identical to the header's
`merkle_root`.

The file is append-only and written in height order, so resume is `last height
+ 1` read from the tail - there is no WAL and no checkpoint replay. A torn final
line is ignored (both fields are validated).

## Header output (`data/headers.csv`, `--store headers`)

One line per block:

```
height,version,prev_hash,merkle_root,time,bits,nonce
```

Those six fields are the complete 80-byte block header, in the order Bitcoin
serialises them. Rebuild the header in little-endian, double-SHA256 it, reverse
the bytes and you get the block hash — so the file is self-verifying without any
network access. About 150 bytes per line.

Same append-only resume rule as `roots.csv`.

## WAL (`data/WIP_<start>.wal`, plain text, fsynced per block)

One line per row: `BLOCK <height>`, then `<counter> <txid>` lines.
Replayed locally on startup — no network. Deleted after its file finalizes.

## `checkpoint.json` (atomic write via temp-file rename)

| field | meaning |
|---|---|
| `last_completed_height` | highest fully processed height |
| `current_file_start_height` | first height of the currently open file |
| `blocks_done_cumulative` | blocks completed in this height range |
| `tx_total_cumulative` | txids collected in this range |
| `bytes_written_total` | finalized parquet bytes so far |
| `files_finalized` | count of closed parquet files |

Resume rule: the WAL wins when present; otherwise resume restarts at
`current_file_start_height` (the open file's pending blocks are re-fetched,
since without the WAL they are unrecoverable). The checkpoint is a summary —
the WAL is the source of truth.

## `stats.json` (atomic write every `--stats-json-interval` seconds, 0 disables)

```json
{
  "timestamp": 1718000000.0,
  "stats": { "...": "see Stats.snapshot() in code: progress, bps, eta, tx averages, latency percentiles, thread states, error counters" },
  "mode": { "mode": "multi", "allowed_workers": 8, "backoff_stage": 0, "retry_in_s": null, "last_event": "...", "rate_multiplier": 1.0 },
  "controls": { "last_key": "-", "paused": false, "adaptive_active": false, "adaptive_target": 4.0 }
}
```

Poll `retry_in_s` for the current 429 pause and
`stats` progress fields for headless monitoring (`--quiet` mode).
