# UNT Alumni Networking Tool - Data Rules and Design Rationale

This document consolidates normalization rules, seniority classification, relevance scoring, and the design rationale behind these decisions.

## 1. Purpose

Raw profile data is inconsistent. The platform applies deterministic rules with guarded AI fallback so filters, analytics, and reporting remain stable over time.

## 2. Normalization Rules

### 2.1 Job Title Normalization

Primary behavior:

- clean whitespace and punctuation noise
- remove trailing location fragments and suffix clutter
- map known aliases to canonical titles
- strip seniority/level fragments when needed for canonical matching
- reject obvious non-title junk values

LLM support exists but deterministic normalization is the base path.

### 2.2 Company Normalization

Primary behavior:

- normalize legal suffix and punctuation variants
- apply deterministic canonical mapping
- preserve clean passthrough names when no canonical mapping exists

LLM fallback is optional and guarded.

### 2.3 Degree Normalization

Two layers are maintained:

- canonical degree name
- grouped degree bucket (`Associate`, `Bachelors`, `Masters`, `Doctorate`, `Other`)

### 2.4 Major Normalization

Output is constrained to approved canonical majors plus `Other`.

Special handling:

- `Computer Science and Engineering` maps to two canonical majors (primary + alt) for filter correctness.

### 2.5 Discipline Inference

Allowed categories:

- `Software, Data, AI & Cybersecurity`
- `Embedded, Electrical & Hardware Engineering`
- `Mechanical Engineering & Manufacturing`
- `Biomedical Engineering`
- `Construction & Engineering Management`
- `Other`

Precedence favors UNT major evidence first, then additional academic/professional signals.

### 2.6 UNT Alumni Status

Derived from UNT education records only:

- `yes`: likely completed UNT education
- `no`: likely still studying / future completion
- `unknown`: insufficient graduation signal

Year-only current-year handling uses a May 15 cutoff.

### 2.7 Working While Studying

Status values:

- `yes`
- `no`
- `currently`
- empty status when insufficient date data exists during scrape stage

Month-level overlap is preferred when available.

## 3. Seniority Classification

Stored buckets:

- `Intern`
- `Mid`
- `Senior`
- `Manager`
- `Executive`

Design choices:

- title-first classifier (recent title is primary)
- experience months are used for audit flagging, not as primary classification
- uncertain cases default to `Mid`
- mismatch flags support manual QA and do not block ingestion

## 4. Relevance Scoring

Each recent role receives a relevance score in `[0, 1]` and a relevance boolean.

Current relevance threshold:

- `is_relevant = true` when score >= `0.45`

Scoring sequence:

1. junk-title short-circuit for obvious non-career service jobs
2. LLM base scoring for non-junk jobs
3. additive boosts for STEM/title-context signals
4. minimum floors for professional/engineering/assistant role classes
5. clamp final score to `[0, 1]`

Rationale:

- include legitimate professional careers beyond only pure engineering titles
- avoid over-rejecting leadership, operations, analyst, or consulting paths
- still exclude clearly non-career service roles

## 5. Fallback and Unknown Conventions

- unresolved degree/major/discipline -> `Other`
- unresolved alumni status -> `unknown`
- malformed LLM outputs -> deterministic fallback
- missing date detail -> unknown/empty status where applicable

## 6. Testing and Verification Areas

High-value test coverage includes:

- alumni status classification
- working-while-studying logic
- major/degree normalization
- discipline classification
- relevance/experience calculations
- backend filter API behavior

## 7. Design Guidance for Stakeholders

For decision-makers:

- standardized fields improve comparability across cohorts and years
- seniority and relevance are directional decision aids, not absolute truth
- flagged profiles are expected and support targeted quality review

For maintainers:

- keep deterministic-first behavior for stability
- keep AI fallback guarded and non-authoritative
- keep allowed category sets explicit and versioned
