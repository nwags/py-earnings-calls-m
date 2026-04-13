# Canonical Consumer-Repo RC Companion Obligations

## Goal

Define the lightweight companion role for the four consumer repos in Wave 7.2.

## Consumer repos are not the center of Wave 7.2

Wave 7.2 is centered on the external package repo/process.
Consumer-repo prompts should be lightweight and focused only on:
- pinning the shared RC,
- running required validations,
- producing signoff/evidence bundle inputs,
- confirming rollback readiness,
- reporting blocker/warning outcomes.

## Repo role summary

### `py-earnings-calls-m`
- pilot consumer-validator for transcript write-path safety.

### `py-news-m`
- pilot consumer-validator for article workflows.

### `py-fed-m`
- validator + signoff for strict non-pilot and strict applicability safety.

### `py-sec-edgar-m`
- conservative validator/signoff for non-pilot and no-dual-authority safety.
