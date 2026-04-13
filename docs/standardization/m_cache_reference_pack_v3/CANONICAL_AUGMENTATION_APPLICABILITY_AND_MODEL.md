# Canonical Augmentation Applicability and Model

## Core rule

Augmentation applies to **text-bearing canonical resources**.
Augmentation does not apply to **numeric-only resources**.

## Applicability matrix

### SEC
- filings: yes
- filing content artifacts: yes
- augmentation already mature: yes

### Earnings
- transcripts: yes
- transcript text artifacts: yes
- forecast numeric snapshots/points: no
- narrative forecast commentary, if present later: yes

### Fed
- documents (speeches/statements/minutes/other text documents): yes
- releases with meaningful text: yes
- numeric series points: no

### News
- article metadata with text snippets: yes
- full article content text: yes
- augmentation transfer from SEC/news pattern: yes

## First canonical augmentation types

Wave 3 should treat these as the minimum shared augmentation types:
- `entity_tagging`
- `temporal_expression_tagging`

Optional future types, not required in Wave 3:
- relation extraction
- event extraction
- topic tagging
- sentiment / stance
- summarization overlays

## Shared augmentation concepts

### Augmentation target
A canonical text-bearing record selected by:
- domain
- resource family
- canonical record key
- text source / artifact selector

### Augmentation run
A single request to produce one or more augmentation outputs for a target.

### Augmentation artifact
Persisted structured output produced by an augmentation run.

### Augmentation provenance
Structured metadata about:
- who/what produced the augmentation,
- on which source text version,
- with which model/settings,
- at what time,
- with what success/degradation outcome.

## Shared augmentation requirements

Every augmentation-capable repo should eventually support:
- deterministic target selection
- augmentation request metadata
- augmentation result metadata
- augmentation event/provenance recording
- persisted structured artifacts
- read-only inspection surfaces

Wave 3 should plan the common contract for all of the above.

## Domain-specific flexibility

The shared model should not force one universal entity schema.
Each domain may extend:
- entity types
- temporal-tag granularity
- domain-specific augmentation fields

But the outer artifact / event / request structure should be shared.

## Acceptance criteria for this slice

Plans should explicitly state:
1. which resource families are augmentation-eligible,
2. which are excluded because they are numeric-only,
3. how existing SEC/news augmentation patterns can transfer into earnings/fed text resources,
4. what common augmentation contracts can be shared safely.
