# Gavrilov, Anguelov, Indyk, Motwani — *Mining the Stock Market: Which Measure Is Best?*

- **PDF:** `p487-gavrilov.pdf`
- **Venue:** ACM SIGKDD (extended abstract), Stanford University.

> **Relevance note:** This is a **data-mining / time-series similarity**
> paper, not an option-pricing study. It does not use Monte Carlo at
> all. It is relevant to the thesis as a reference on similarity measures
> for equity return time series — a topic that intersects with the
> *analogue* / k-NN simulation method used in this work (`sim_analogue`),
> which depends on a notion of "what does today's recent return state
> look like".

## Methodology (no Monte Carlo)
Comparative study of **similarity measures for clustering of equity
price time series**:

- Euclidean distance on raw or globally normalised series.
- Comparison after applying global $z$-score normalisation.
- Comparison after taking first differences (returns) or their
  normalised version.
- A novel **piecewise normalisation** measure that splits each series
  into blocks and normalises within each block independently.

Each measure feeds an off-the-shelf clustering algorithm; the resulting
clusters are compared against a "ground-truth" industry-sector labelling.

## Type of data
**Empirical.** 500 S&P 500 stocks, daily prices for the year 1998 (252
observations per stock), with 102 industry-sector cluster labels used
as ground truth.

## Computational setup
**Single machine.** No parallelism or distributed computation is
discussed; the dataset is small.

## Key contribution
Identifies that **the choice of similarity measure** matters as much
as the choice of clustering algorithm:

- Comparing normalised first differences ("normalised derivatives")
  beats comparing the raw or globally normalised series — a phenomenon
  long known to financial practitioners but rarely formalised in the
  data-mining literature.
- The proposed **piecewise normalisation** measure outperforms global
  normalisation, presumably by adapting to local regime changes.

## Strengths
- Direct, comparative experimental design on a clean and widely
  available dataset.
- Articulates the novel piecewise-normalisation idea simply enough to
  reproduce.
- Uses ground-truth labels (industry sectors) as an external validity
  signal.

## Limitations
- Single year (1998) and single market (US large-caps) — generalisation
  to other regimes / markets is untested.
- Ignores cross-sectional correlations (only pairwise similarity).
- Industry-sector labels are only a proxy for "true" similarity and
  may themselves be noisy.
- The clustering algorithm itself is not the focus; the choice can
  interact with the similarity measure in ways not explored.
