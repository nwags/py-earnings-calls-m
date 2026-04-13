# Canonical Command Model

## Goal

Introduce a shared top-level operator surface while preserving current project CLIs as compatibility aliases.

## Shared top-level CLI

The long-term canonical entrypoint is:

```bash
m-cache <domain> <family-or-command> [subcommand] [options]
```

Supported domains in the first merged model:

- `sec`
- `earnings`
- `fed`
- `news`

Examples:

```bash
m-cache sec refdata refresh
m-cache earnings transcripts import-bulk --adapter local_tabular --dataset ./data/transcripts.jsonl
m-cache fed documents import-history --year 2019 --limit 50
m-cache news articles backfill --provider gdelt_recent --days 2
```

## Compatibility rule

Existing CLIs remain valid during migration:

- `py-sec-edgar ...`
- `py-earnings-calls ...`
- `py-fed ...`
- `py-news ...`

Each existing CLI should be implemented as either:

1. a thin alias to the same underlying command handlers used by `m-cache`, or
2. a compatibility wrapper that forwards into the new shared dispatcher.

## Shared command families

These command families should converge strongly across domains, even when a family is initially unimplemented in one repo:

- `refdata`
- `providers`
- `lookup`
- `resolve`
- `monitor`
- `reconcile`
- `storage`
- `audit`
- `api`

### Canonical meanings

#### `refdata`
Build or refresh canonical non-payload reference data used by runtime logic.

#### `providers`
Inspect provider policies, limits, capability flags, and effective configuration.

#### `lookup`
Build and query derived local lookup projections.

#### `resolve`
Run bounded local-first record resolution workflows for one canonical identity.

#### `monitor`
Perform bounded freshness checks and optional warming loops.

#### `reconcile`
Compare expected vs actual local state, persist discrepancy artifacts, and optionally warm missing targets.

#### `storage`
Inspect or migrate storage-layout state without changing domain identities.

#### `audit`
Generate read-only reports and bundles for operator validation.

#### `api`
Serve the domain API or inspect API/runtime configuration.

## Domain resource families

These remain domain-specific because they reflect genuine model differences.

### SEC

- `backfill`
- `filing-parties`
- `aug` (canonical)
- `augmentations` (backward-compatible alias)

### Earnings

- `transcripts`
- `forecasts`

### Fed

- `documents`
- `releases`
- `series`

### News

- `articles`

## Canonical alias rules

### Rule 1
`augmentations` in SEC becomes `aug` canonically, but `augmentations` must remain a valid alias.

### Rule 2
Current project command names that already match the canonical meaning should be preserved.

### Rule 3
Where a command family exists in one repo but not another, the absent repo should reserve the family name rather than inventing a different synonym.

## Global flags

The following global flags should be standardized for all runtime work commands when applicable:

- `--summary-json`
- `--progress-json`
- `--progress-heartbeat-seconds`
- `--verbose`
- `--quiet`
- `--log-level`
- `--log-file`

### Output rules

- `--summary-json` must emit clean JSON on stdout only.
- `--progress-json` must emit NDJSON progress events on stderr only.
- Human-oriented output must not corrupt machine-readable stdout.

## Command implementation rule

The first implementation wave should not deeply rewrite domain internals.
Instead:

1. add the canonical family names and aliases
2. route them into existing pipeline/service functions
3. standardize outputs before broader refactors

## Suggested first-wave shape by domain

### SEC

```bash
m-cache sec refdata refresh
m-cache sec index refresh
m-cache sec backfill
m-cache sec filing-parties query
m-cache sec lookup refresh
m-cache sec lookup query
m-cache sec monitor poll
m-cache sec monitor loop
m-cache sec reconcile run
m-cache sec aug submission
m-cache sec aug events
m-cache sec api serve
```

### Earnings

```bash
m-cache earnings refdata fetch-sec-sources
m-cache earnings refdata refresh
m-cache earnings transcripts import-bulk
m-cache earnings transcripts backfill
m-cache earnings transcripts audit-datetime
m-cache earnings forecasts refresh-daily
m-cache earnings lookup refresh
m-cache earnings lookup query
m-cache earnings monitor poll
m-cache earnings monitor loop
m-cache earnings reconcile run
m-cache earnings providers list
m-cache earnings resolve transcript
m-cache earnings resolve forecast-snapshot
m-cache earnings api serve
```

### Fed

```bash
m-cache fed refdata refresh
m-cache fed documents import-history
m-cache fed documents backfill-rss
m-cache fed releases refresh
m-cache fed series refresh
m-cache fed lookup refresh
m-cache fed lookup query
m-cache fed monitor poll
m-cache fed monitor loop
m-cache fed reconcile run
m-cache fed providers list
m-cache fed resolve document
m-cache fed resolve release
m-cache fed resolve series
m-cache fed api serve
```

### News

```bash
m-cache news refdata refresh
m-cache news providers list
m-cache news articles import-history
m-cache news articles backfill
m-cache news articles fetch-content
m-cache news articles inspect
m-cache news lookup refresh
m-cache news lookup query
m-cache news resolve article
m-cache news resolution events
m-cache news audit report
m-cache news audit compare
m-cache news audit bundle
m-cache news cache rebuild-layout
m-cache news api serve
```

## Acceptance criteria for this slice

All four repos pass this slice when:

1. `m-cache <domain>` dispatch exists
2. compatibility CLIs still work
3. standardized global runtime flags behave consistently
4. family names and aliases are aligned with this document
5. no repo invents a new synonym for a reserved family name without explicit approval
