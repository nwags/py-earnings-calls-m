# Migration Checklist (Wave 5.1)

This is a planning-first checklist.

## Package layout normalization
- [ ] Normalize to `m_cache_shared/augmentation/...`
- [ ] Preserve temporary compatibility imports where needed
- [ ] Keep local wrappers intact

## Export normalization
- [ ] Normalize model exports
- [ ] Normalize enum/vocab exports
- [ ] Normalize validator exports
- [ ] Normalize schema-loader exports
- [ ] Normalize packer exports
- [ ] Normalize CLI helper exports

## Test/doc normalization
- [ ] Align shared-package test naming
- [ ] Align migration note structure
- [ ] Align package-usage examples in docs

## Behavior freeze
- [ ] Pilot behavior unchanged
- [ ] Non-pilot behavior unchanged
- [ ] Applicability unchanged
- [ ] Authority behavior unchanged
