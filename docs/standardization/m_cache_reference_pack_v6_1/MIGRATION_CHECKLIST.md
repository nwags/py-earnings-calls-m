# Migration Checklist (Wave 6.1)

This is a planning-first checklist.

## External identity convergence
- [ ] Normalize to one external distribution name
- [ ] Normalize to one external import root
- [ ] Normalize to one pin file path
- [ ] Normalize to one shared RC tag

## Shim contract convergence
- [ ] Normalize source-mode env var name
- [ ] Normalize source-mode values and meanings
- [ ] Normalize external-root override name
- [ ] Normalize fallback behavior
- [ ] Keep one first-hop facade/shim per repo

## Behavior freeze
- [ ] Pilot behavior unchanged
- [ ] Non-pilot behavior unchanged
- [ ] Applicability unchanged
- [ ] Authority behavior unchanged

## Validation
- [ ] Define shared RC validation checks
- [ ] Define repo integration checks
- [ ] Define rollback rule
