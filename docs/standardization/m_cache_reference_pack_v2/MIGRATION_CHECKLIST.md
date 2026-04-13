# Migration Checklist (Wave 2)

## Repository-level success criteria

### 1. Provider operations
- [ ] Add `m-cache <domain> providers list`
- [ ] Add `m-cache <domain> providers show --provider <id>`
- [ ] Keep provider inspection offline and deterministic
- [ ] Avoid inventing a synonym if `show` is sufficient

### 2. Rate-limit and degradation visibility
- [ ] Expose rate-limit policy and effective provider policy clearly
- [ ] Surface retry counts where remote-capable work occurs
- [ ] Surface defer outcomes transparently when quota/policy blocks work
- [ ] Preserve legacy defaults on base CLIs

### 3. Resolve semantics
- [ ] Align `local_only`, `resolve_if_missing`, and `refresh_if_stale`
- [ ] Fail transparently for unsupported modes
- [ ] Keep list/browse endpoints local-only by default
- [ ] Keep detail/content remote resolution explicit

### 4. API transparency
- [ ] Additive response metadata for provider/resolve outcomes exists on remote-capable endpoints
- [ ] Current status-code behavior remains compatible unless already safely additive
- [ ] Existing endpoint paths remain stable where practical

### 5. Events and outputs
- [ ] Summary/progress outputs surface provider/rate-limit/defer outcomes
- [ ] `resolution_events.parquet` carries canonical provider/rate-limit fields where appropriate
- [ ] No broad historical artifact rewrite is introduced

### 6. Tests
- [ ] Provider list/show tests
- [ ] Resolve-mode behavior tests
- [ ] Rate-limit/defer telemetry tests
- [ ] API transparency tests
- [ ] Legacy compatibility tests

## Pause point after Wave 2

Pause after Wave 2 and compare all four repos side by side again.

Do not begin shared-package extraction until:
- provider operations are aligned,
- rate-limit and defer visibility are aligned,
- API transparency semantics are aligned,
- unsupported resolution modes are handled consistently.
