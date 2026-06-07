# Moldovan, Moca, Rusu — *Analysis and Design Insights for an E-Finance Platform Using Parallel Processing*

- **PDF:** `ACA-31.pdf`
- **Venue:** Conference paper (ACA-31), Babeș-Bolyai University of Cluj-Napoca.

## Methodology (no specific Monte Carlo variant)
This is an **architecture / requirements-engineering** paper that
proposes the design of an e-finance services platform. Computational
methods cited as candidate workloads for the platform include:

- text mining of financial news,
- neural networks,
- genetic algorithms,
- (more generally) compute-intensive predictive models exposed as
  e-services.

Monte Carlo simulation per se is *not* the focus — the paper argues
that these various computationally heavy models should be parallelised
on a shared distributed-computing back-end. No specific MC algorithm is
proposed or evaluated.

## Type of data
Conceptual. The paper does not present an empirical study. It cites
public statistics on investor performance (e.g. the often-quoted
"80–90% of retail investors lose money") as motivation.

## Computational setup
Proposes a generic **distributed-computing infrastructure** for parallel
execution of services. The specific framework (cluster, grid, cloud,
MapReduce, Spark) is left open; the paper is platform-agnostic at the
design level.

## Key contribution
- Identifies three concrete pain points for retail and corporate
  investors: lack of integrated analytical tools, absence of automated
  real-time reactivity, and missing mechanisms for cleaning data from
  heterogeneous sources.
- Sketches a modular design where each pain point is addressed by an
  e-service whose computational core can be scaled horizontally.

## Strengths
- Useful as a high-level architectural reference for any project that
  wraps quantitative models into a multi-user services platform — the
  service-oriented design philosophy aligns closely with the FastAPI +
  worker + UI architecture of this thesis.
- Explicitly motivates the *systems* layer of an e-finance product,
  which is rarely addressed in option-pricing literature.

## Limitations
- No concrete algorithms, no benchmarks, no implementation details.
- Cites "parallel processing" broadly without committing to a framework
  or measuring speed-up.
- Does not cover security, authentication, governance — all of which
  are first-class concerns of a real e-finance platform.
- Treatment of efficient-market theory and behavioural-finance findings
  is brief and qualitative.
