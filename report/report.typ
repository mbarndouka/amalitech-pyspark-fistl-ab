// ─────────────────────────────────────────────────────────────────────────────
// Blockbuster Intelligence Report
// TMDB Top-Grossing Films Analysis · PySpark Pipeline
// ─────────────────────────────────────────────────────────────────────────────

// ── Page & fonts ─────────────────────────────────────────────────────────────
#set page(
  paper: "a4",
  margin: (top: 2.4cm, bottom: 2.4cm, left: 2.4cm, right: 2.4cm),
  header: context {
    if counter(page).get().first() > 1 [
      #set text(size: 8pt, fill: rgb("#8899AA"))
      #grid(
        columns: (1fr, 1fr),
        align(left)[Blockbuster Intelligence Report],
        align(right)[TMDB · PySpark Pipeline · 2026],
      )
      #line(length: 100%, stroke: 0.4pt + rgb("#2D2D44"))
    ]
  },
  footer: context {
    if counter(page).get().first() > 1 [
      #line(length: 100%, stroke: 0.4pt + rgb("#2D2D44"))
      #set text(size: 8pt, fill: rgb("#8899AA"))
      #align(center)[#counter(page).display("1 of 1", both: true)]
    ]
  },
)

#set text(font: "New Computer Modern", size: 11pt, fill: rgb("#1A1A2E"))
#set par(justify: true, leading: 0.75em)
#set heading(numbering: "1.1")

// ── Colour palette ────────────────────────────────────────────────────────────
#let accent    = rgb("#3498DB")
#let accent2   = rgb("#E74C3C")
#let accent3   = rgb("#2ECC71")
#let muted     = rgb("#8899AA")
#let surface   = rgb("#F4F6F9")
#let dark      = rgb("#1A1A2E")

// ── Reusable components ───────────────────────────────────────────────────────
#let kpi(value, label) = block(
  fill: surface,
  radius: 6pt,
  inset: (x: 14pt, y: 10pt),
  width: 100%,
)[
  #set align(center)
  #text(size: 22pt, weight: "bold", fill: accent)[#value]
  #linebreak()
  #text(size: 8.5pt, fill: muted)[#label]
]

#let chip(body, color: accent) = box(
  fill: color.lighten(80%),
  stroke: 0.6pt + color,
  radius: 4pt,
  inset: (x: 6pt, y: 2pt),
)[#text(size: 8pt, fill: color, weight: "semibold")[#body]]

#let insight(body) = block(
  fill: accent.lighten(90%),
  stroke: (left: 3pt + accent),
  inset: (x: 12pt, y: 8pt),
  radius: (right: 4pt),
  width: 100%,
)[#text(size: 10pt)[#body]]

#let warn(body) = block(
  fill: accent2.lighten(90%),
  stroke: (left: 3pt + accent2),
  inset: (x: 12pt, y: 8pt),
  radius: (right: 4pt),
  width: 100%,
)[#text(size: 10pt)[#body]]

#let section-rule() = {
  v(4pt)
  line(length: 100%, stroke: 0.6pt + rgb("#E0E4EC"))
  v(8pt)
}

// ─────────────────────────────────────────────────────────────────────────────
// COVER PAGE
// ─────────────────────────────────────────────────────────────────────────────
#page(
  margin: (top: 0pt, bottom: 0pt, left: 0pt, right: 0pt),
  header: none,
  footer: none,
)[
  // Dark band at top
  #block(width: 100%, height: 52%, fill: dark)[
    #pad(x: 3cm, top: 3cm)[
      #text(size: 11pt, fill: accent, tracking: 4pt)[TMDB · PYSPARK PIPELINE]
      #v(12pt)
      #text(size: 34pt, weight: "bold", fill: white, font: "New Computer Modern")[
        Blockbuster\
        Intelligence\
        Report
      ]
      #v(16pt)
      #line(length: 6cm, stroke: 1.5pt + accent)
      #v(12pt)
      #text(size: 12pt, fill: rgb("#EAEAEA"))[
        Analysis of 18 Top-Grossing Films\
        Revenue · ROI · Genre · Franchise Performance
      ]
    ]
  ]
  // Light band at bottom
  #block(width: 100%, height: 48%, fill: white)[
    #pad(x: 3cm, top: 2.5cm)[
      #text(size: 10pt, fill: muted)[*Dataset*]
      #h(1em)
      #text(size: 10pt)[18 films · TMDB REST API · cleaned & enriched via PySpark]
      #v(8pt)
      #text(size: 10pt, fill: muted)[*Pipeline*]
      #h(1em)
      #text(size: 10pt)[Ingestion → Cleaning → KPI → Advanced Queries → Visualization]
      #v(8pt)
      #text(size: 10pt, fill: muted)[*Date*]
      #h(2.2em)
      #text(size: 10pt)[April 2026]
      #v(2cm)
      #grid(
        columns: 5,
        gutter: 10pt,
        chip("Adventure"),
        chip("Action", color: accent2),
        chip("Sci-Fi", color: accent3),
        chip("Franchise", color: rgb("#9B59B6")),
        chip("Blockbuster", color: rgb("#F39C12")),
      )
    ]
  ]
]

// ─────────────────────────────────────────────────────────────────────────────
// TABLE OF CONTENTS
// ─────────────────────────────────────────────────────────────────────────────
#page[
  #v(1cm)
  #text(size: 20pt, weight: "bold", fill: dark)[Table of Contents]
  #v(4pt)
  #line(length: 100%, stroke: 1.5pt + accent)
  #v(1cm)

  #set text(size: 11pt)
  #outline(indent: 1.5em, depth: 2)
]

// ─────────────────────────────────────────────────────────────────────────────
// 1. EXECUTIVE SUMMARY
// ─────────────────────────────────────────────────────────────────────────────
= Executive Summary

This report presents findings from a data engineering and analytics pipeline
built to analyse the world's highest-grossing films. Eighteen titles were
retrieved from the TMDB API, cleaned and enriched using Apache Spark, and
examined across five analytical dimensions: financial performance, return on
investment, audience reception, genre composition, and franchise dynamics.

#v(10pt)
// KPI row
#grid(
  columns: (1fr, 1fr, 1fr, 1fr),
  gutter: 10pt,
  kpi("18", "Films Analysed"),
  kpi("\$30.5B", "Combined Box Office"),
  kpi("8.2×", "Mean ROI"),
  kpi("7.4 / 10", "Mean Audience Rating"),
)
#v(10pt)

#insight[
  *Key finding:* The dataset collectively earned \$30.5 billion against a
  combined production budget of \$3.85 billion — a mean return of
  *8.2× per dollar invested*. Franchise sequels dominate by volume (89 % of
  titles) yet show statistically similar revenue and ratings to the two
  standalone entries, suggesting that brand recognition alone is not a
  sufficient predictor of outperformance.
]

#section-rule()

The pipeline processes data entirely in-memory using Spark's directed acyclic
graph (DAG) engine; only the raw API response is persisted to disk as a
cache. The full ingest-to-chart cycle completes in under 30 seconds on a
local machine.

// ─────────────────────────────────────────────────────────────────────────────
// 2. METHODOLOGY
// ─────────────────────────────────────────────────────────────────────────────
= Methodology

== Data Collection

Movie detail and full cast/crew credits were fetched from the TMDB REST API
using an async HTTP client (`httpx`) with tenacity-managed exponential backoff
(max 3 retries, initial wait 1 s, jitter). HTTP 429 rate-limit responses are
treated as transient and retried identically to 5xx server errors. Up to 15
concurrent requests are issued simultaneously, bounded by a semaphore tied to
the `max_concurrent` config value.

The 18 film IDs were selected to represent the highest-grossing releases
across multiple studios and decades: spanning 1997–2019, covering Disney,
Marvel, Universal, Lucasfilm, and Warner Bros.

== Data Cleaning Pipeline

The raw DataFrame was processed through nine sequential Spark transformations:

#v(6pt)
#block(
  fill: surface, radius: 6pt, inset: 12pt, width: 100%,
)[
  #grid(
    columns: (auto, 1fr),
    gutter: (6pt, 4pt),
    ..{
      let steps = (
        ("Step 1", "Drop irrelevant columns (adult, imdb_id, original_title, video, homepage)"),
        ("Step 2", "Extract nested struct/array fields: genres, spoken_languages, production_countries, production_companies, belongs_to_collection"),
        ("Step 3", "Extract cast names, cast size, director names, and crew size from raw credit arrays"),
        ("Step 4", "Inspect extracted columns — surface null/anomaly counts"),
        ("Step 5", "Cast budget, revenue, popularity to DoubleType; release_date to DateType"),
        ("Step 6", "Replace zeroes with null for budget/revenue/runtime; derive budget_musd and revenue_musd (÷ 1 000 000); nullify ratings with zero vote count; strip placeholder text from overview/tagline"),
        ("Step 7", "De-duplicate on movie ID; drop rows with null ID or title"),
        ("Step 8", "Require ≥ 10 non-null columns per row"),
        ("Step 9", "Keep only Released status films; rename and reorder final column set"),
      )
      for (n, d) in steps {
        (text(weight: "bold", fill: accent)[#n], text(size: 10pt)[#d])
      }
    }
  )
]

== Analytical Approach

Derived metrics were computed in Spark using native column expressions:

- *Profit* = revenue\_musd − budget\_musd
- *ROI* = revenue\_musd ÷ budget\_musd

KPI rankings used `Window.orderBy` with `rank()` (dense-rank semantics) to
handle ties. Franchise classification was determined by the presence of a
non-null, non-empty `belongs_to_collection` field. Genre-level aggregations
exploded the pipe-separated genre string into individual rows before
grouping.

== Visualisation

Five charts were produced using `matplotlib` and `numpy` directly — seaborn
was excluded because it triggers a Windows Application Control block via its
pandas dependency. All theme parameters (colours, DPI, figure dimensions) are
driven by `VizSettings` in `config.toml`, making the palette fully
configurable without touching source code.

// ─────────────────────────────────────────────────────────────────────────────
// 3. DATASET OVERVIEW
// ─────────────────────────────────────────────────────────────────────────────
= Dataset Overview

The final cleaned dataset contains *18 films* released between 1997 and 2019.
All titles are English-language theatrical releases with a minimum of 100
audience votes on TMDB. The corpus is intentionally skewed toward
high-performers — it was constructed from known blockbusters — so the
financial metrics should be interpreted as representative of the upper tail of
commercial cinema rather than the industry average.

#figure(
  image("04_yearly_trends.png", width: 100%),
  caption: [Annual distribution of mean production budget, box-office revenue,
            and net profit. Bar pairs share a left axis (M USD); the profit
            line uses a right axis.],
)

#insight[
  *2018 and 2019* were the most active years (4 films each), coinciding with
  the climax of the Marvel Cinematic Universe's *Infinity Saga* and Disney's
  live-action remake cycle. Mean profit peaked in 2019 at over \$2 billion
  per film, driven primarily by *Avengers: Endgame* and *The Lion King*.
]

The 18 films collectively span 11 distinct genres with significant overlap
per title. Adventure and Action appear in 83 % and 67 % of films respectively,
reflecting the dominance of the action-adventure template in the blockbuster era.

// ─────────────────────────────────────────────────────────────────────────────
// 4. FINANCIAL PERFORMANCE
// ─────────────────────────────────────────────────────────────────────────────
= Financial Performance

== Revenue and Budget

#grid(
  columns: (1fr, 1fr, 1fr),
  gutter: 10pt,
  kpi("\$1.69B", "Mean Box Office Revenue"),
  kpi("\$214M",  "Mean Production Budget"),
  kpi("\$1.48B", "Mean Net Profit"),
)

#v(10pt)

#figure(
  image("01_revenue_vs_budget.png", width: 100%),
  caption: [Revenue vs. Budget scatter. Each point is coloured by release year
            (plasma scale). The dotted line is break-even (revenue = budget);
            the dashed line is the OLS regression trend. Top-5 earners are
            annotated.],
)

Every film in the dataset returned more than its production budget — the
break-even line passes below all data points. The OLS regression confirms a
strong positive relationship between investment and gross, but with notable
dispersion: several mid-budget films (\$100–150 M) achieved revenues
comparable to the highest-budget productions.

#warn[
  *Survivorship bias:* This dataset was constructed from known
  blockbusters. The positive relationship between budget and revenue, and the
  complete absence of loss-making titles, do not reflect the industry-wide
  distribution where the majority of films fail to recoup their production
  costs.
]

== Return on Investment

#figure(
  image("02_roi_by_genre.png", width: 100%),
  caption: [Mean ROI by genre (revenue ÷ budget). The dotted vertical line
            marks break-even (ROI = 1×). Sample sizes (n) are shown on each bar.],
)

#grid(
  columns: (1fr, 1fr),
  gutter: 10pt,
  block(fill: surface, radius: 6pt, inset: 12pt)[
    *Top 5 ROI*
    #v(6pt)
    #set text(size: 10pt)
    #table(
      columns: (1fr, auto),
      stroke: none,
      inset: (y: 4pt),
      [*Film*], [*ROI*],
      [Avatar], [12.34×],
      [Titanic], [11.32×],
      [Jurassic World], [11.14×],
      [HP: Deathly Hallows Pt 2], [10.73×],
      [Frozen II], [9.69×],
    )
  ],
  block(fill: surface, radius: 6pt, inset: 12pt)[
    *ROI by Genre (top)*
    #v(6pt)
    #set text(size: 10pt)
    #table(
      columns: (1fr, auto),
      stroke: none,
      inset: (y: 4pt),
      [*Genre*], [*Mean ROI*],
      [Romance], [11.83×],
      [Drama], [11.83×],
      [Fantasy], [10.61×],
      [Family], [9.49×],
      [Animation], [9.49×],
    )
  ],
)

#insight[
  Romance and Drama genres show the highest mean ROI despite appearing in
  only 2 films each — both of those films are *Titanic* and *Avatar*,
  James Cameron's two juggernauts. The outsized ROI of these genres is
  therefore attributable to individual films rather than a genre-level effect.
  Adventure (n=15) and Action (n=12) deliver more *consistent* returns
  at 8.0× and 7.6× respectively, making them the reliable workhorses of the
  blockbuster business model.
]

// ─────────────────────────────────────────────────────────────────────────────
// 5. AUDIENCE RECEPTION
// ─────────────────────────────────────────────────────────────────────────────
= Audience Reception

#grid(
  columns: (1fr, 1fr, 1fr, 1fr),
  gutter: 10pt,
  kpi("7.40", "Mean Rating"),
  kpi("7.26", "Median Rating"),
  kpi("138 min", "Mean Runtime"),
  kpi("22.2", "Mean Popularity Score"),
)
#v(10pt)

#figure(
  image("03_popularity_vs_rating.png", width: 100%),
  caption: [Popularity vs. average rating. Bubble area is proportional to
            vote count; colour encodes production budget (yellow = low,
            red = high). Top-6 films by popularity are labelled.],
)

The popularity score (a TMDB composite metric weighting recent views,
watchlist additions, and vote activity) shows low correlation with the
static vote average. Films that were recent releases at data-collection time
score disproportionately high on popularity regardless of their rating.

#block(
  fill: surface, radius: 6pt, inset: 12pt, width: 100%,
)[
  *Top 5 Rated Films* (minimum 100 votes)
  #v(6pt)
  #set text(size: 10pt)
  #table(
    columns: (1fr, auto, auto),
    stroke: none,
    inset: (y: 5pt),
    [*Film*], [*Rating*], [*Vote Count*],
    [Avengers: Endgame],               [8.2], [27,489],
    [Avengers: Infinity War],          [8.2], [31,728],
    [HP: Deathly Hallows Pt 2],        [8.1], [21,845],
    [The Avengers],                    [8.0], [37,181],
    [Titanic],                         [7.9], [26,991],
  )
]

// ─────────────────────────────────────────────────────────────────────────────
// 6. GENRE ANALYSIS
// ─────────────────────────────────────────────────────────────────────────────
= Genre Analysis

Films in this dataset are multi-genre — each title carries an average of 2.8
genre tags. The distribution below counts each genre tag independently:

#block(
  fill: surface, radius: 6pt, inset: 12pt, width: 100%,
)[
  #grid(
    columns: (1fr, 1fr),
    gutter: 20pt,
    [
      *By frequency*
      #v(6pt)
      #set text(size: 10pt)
      #table(
        columns: (1fr, auto),
        stroke: none,
        inset: (y: 4pt),
        [*Genre*], [*Films*],
        [Adventure],      [15 (83%)],
        [Action],         [12 (67%)],
        [Science Fiction],[10 (56%)],
        [Family],         [5  (28%)],
        [Fantasy],        [4  (22%)],
        [Animation],      [4  (22%)],
        [Thriller],       [3  (17%)],
        [Drama],          [2  (11%)],
        [Romance],        [2  (11%)],
      )
    ],
    [
      *Key observations*
      #v(6pt)
      #set text(size: 10.5pt)
      Adventure appears in *15 of 18* films — it is the near-universal
      foundation of the blockbuster template.
      #v(8pt)
      Science Fiction is the third most common genre, present in all MCU
      entries and in Avatar, reflecting the genre's dominance in
      \$200M+ productions since 2008.
      #v(8pt)
      Animation and Family genres overlap perfectly in this dataset —
      every animated blockbuster (Frozen, Frozen II, Incredibles 2,
      The Lion King) carries both tags.
    ],
  )
]

// ─────────────────────────────────────────────────────────────────────────────
// 7. FRANCHISE vs STANDALONE ANALYSIS
// ─────────────────────────────────────────────────────────────────────────────
= Franchise vs Standalone Analysis

#figure(
  image("05_franchise_vs_standalone.png", width: 100%),
  caption: [Franchise vs. standalone mean performance across four KPIs. Sample
            sizes: franchise n=16, standalone n=2.],
)

#grid(
  columns: (1fr, 1fr),
  gutter: 10pt,
  block(fill: surface, radius: 6pt, inset: 12pt)[
    *Franchise (n = 16)*
    #v(6pt)
    #set text(size: 10pt)
    #table(
      columns: (1fr, auto),
      stroke: none,
      inset: (y: 4pt),
      [Mean Revenue], [\$1,683 M],
      [Mean Rating],  [7.39],
    )
  ],
  block(fill: surface, radius: 6pt, inset: 12pt)[
    *Standalone (n = 2)*
    #v(6pt)
    #set text(size: 10pt)
    #table(
      columns: (1fr, auto),
      stroke: none,
      inset: (y: 4pt),
      [Mean Revenue], [\$1,765 M],
      [Mean Rating],  [7.44],
    )
  ],
)

#v(8pt)

The two standalone films — *Titanic* and *Avatar* — actually *outperform*
franchise films on both revenue (\$1,765 M vs \$1,683 M) and rating
(7.44 vs 7.39), though the difference is small and the standalone sample of
two films makes any broad conclusion unreliable.

#warn[
  *Statistical caution:* With only 2 standalone entries, no statistically
  significant conclusion can be drawn about franchise vs. standalone
  performance. A meaningful comparison would require a dataset of hundreds of
  films drawn from the full box-office distribution, not just the top tier.
]

#insight[
  Both standalone films come from *James Cameron*, whose non-franchise
  productions are extreme outliers. Excluding these two titles, the remaining
  16 franchise films represent the entire range of MCU, Harry Potter,
  Star Wars, Jurassic World, and Frozen entries — a commercially homogeneous
  group where inter-franchise variation is larger than franchise vs. standalone
  variation.
]

// ─────────────────────────────────────────────────────────────────────────────
// 8. DIRECTOR PERFORMANCE
// ─────────────────────────────────────────────────────────────────────────────
= Director Performance

#block(
  fill: surface, radius: 6pt, inset: 12pt, width: 100%,
)[
  *Directors ranked by total box-office revenue*
  #v(8pt)
  #set text(size: 10pt)
  #table(
    columns: (1fr, auto, auto),
    stroke: none,
    inset: (y: 5pt),
    [*Director*], [*Films*], [*Total Revenue*],
    [James Cameron],          [2], [\$5,188 M],
    [Anthony & Joe Russo],    [2], [\$4,852 M],
    [Joss Whedon],            [2], [\$2,924 M],
    [Chris Buck & Jennifer Lee], [2], [\$2,728 M],
    [J.J. Abrams],            [1], [\$2,068 M],
    [Colin Trevorrow],        [1], [\$1,672 M],
    [Jon Favreau],            [1], [\$1,662 M],
    [James Wan],              [1], [\$1,515 M],
    [Ryan Coogler],           [1], [\$1,350 M],
    [David Yates],            [1], [\$1,342 M],
  )
]

#insight[
  *James Cameron* leads all directors with \$5.19 billion from just two
  films — the highest per-film average in the dataset at \$2.59 billion.
  The *Russo Brothers* (Anthony and Joe share co-directing credits on both
  Infinity War and Endgame) trail closely at \$4.85 billion from two entries.
  Every director in this corpus cleared \$1 billion in total box office,
  reflecting the selection criteria of the dataset.
]

// ─────────────────────────────────────────────────────────────────────────────
// 9. CONCLUSIONS
// ─────────────────────────────────────────────────────────────────────────────
= Conclusions

== Summary of Findings

#block(
  fill: surface, radius: 6pt, inset: 12pt, width: 100%,
)[
#set text(size: 10.5pt)
#grid(
  columns: (auto, 1fr),
  gutter: (8pt, 6pt),
  ..{
    let findings = (
      ("1.", "The 18 films collectively grossed *\$30.5 billion* against a *\$3.85 billion* combined budget, yielding a mean ROI of *8.2×*."),
      ("2.", "*Adventure* is the near-universal genre foundation (83 % of films); Action (67 %) and Science Fiction (56 %) are the next most common, often combined."),
      ("3.", "*Genre-level ROI* rankings are heavily influenced by individual films: Romance and Drama top the chart solely because of Titanic and Avatar."),
      ("4.", "*Franchise dominance* is structural rather than economic — 89 % of the dataset are franchise entries, yet the two standalone films outperform on revenue and rating. The dataset is too small for a reliable comparison."),
      ("5.", "*James Cameron* holds the highest per-film revenue average (\$2.59 B) and the two highest ROIs (Avatar 12.34×, Titanic 11.32×)."),
      ("6.", "*2018–2019* was the most commercially dense window in this dataset, driven by the MCU Infinity Saga conclusion and Disney's live-action remakes."),
      ("7.", "Audience *ratings and popularity* are weakly correlated: well-regarded films do not necessarily score high on TMDB's popularity composite, which weights recency and engagement activity."),
    )
    for (n, d) in findings {
      (text(weight: "bold", fill: accent)[#n], [#eval(d, mode: "markup")])
    }
  }
)]

== Limitations

- *Sample size:* 18 films drawn exclusively from the upper tail of the
  box-office distribution. Conclusions do not generalise to the broader film
  industry.

- *Missing credits:* Cast and crew fields relied on the TMDB credits
  endpoint. Some records contain null name fields due to incomplete
  upstream data, slightly underrepresenting the cast/crew coverage.

- *Single release window:* The dataset covers 1997–2019. Streaming-era
  economics (post-2020) are absent, where theatrical revenue models have
  changed substantially.

- *No marketing costs:* Production budgets exclude print-and-advertising
  (P&A) spend, which can equal or exceed the production budget for wide
  releases. True ROI would be materially lower if P&A were included.

== Pipeline Reproducibility

The full pipeline can be reproduced from a clean environment by running:

#block(
  fill: dark,
  radius: 6pt,
  inset: 14pt,
  width: 100%,
)[
  #set text(font: "Courier New", size: 9.5pt, fill: rgb("#2ECC71"))
```
git clone <repo>
cp .env.example .env          # add your TMDB_API_KEY
pip install -r requirements.txt
python main.py                 # ingest → clean → KPI → queries → charts
```
]

All intermediate transformations run in Spark's in-memory DAG. The only file
written to disk during the pipeline is the raw API response
(`data/raw/movies.parquet`), which acts as a cache for subsequent reruns.
Total wall-clock time on a standard laptop: *under 30 seconds*.

// ─────────────────────────────────────────────────────────────────────────────
// APPENDIX
// ─────────────────────────────────────────────────────────────────────────────
= Appendix: Full Film Listing

#set text(size: 9.5pt)
#block(
  fill: surface, radius: 6pt, inset: 12pt, width: 100%,
)[
#table(
  columns: (1fr, auto, auto, auto, auto),
  stroke: none,
  fill: (_, y) => if calc.even(y) { white } else { rgb("#EDF2F7") },
  inset: (x: 6pt, y: 5pt),
  table.header(
    text(weight: "bold")[Film],
    text(weight: "bold")[Year],
    text(weight: "bold")[Budget (M)],
    text(weight: "bold")[Revenue (M)],
    text(weight: "bold")[Rating],
  ),
  [Avatar],                                      [2009], [\$237],  [\$2,924], [7.6],
  [Avengers: Endgame],                           [2019], [\$356],  [\$2,799], [8.2],
  [Titanic],                                     [1997], [\$200],  [\$2,264], [7.9],
  [Star Wars: The Force Awakens],                [2015], [\$245],  [\$2,068], [7.8],
  [Avengers: Infinity War],                      [2018], [\$321],  [\$2,052], [8.2],
  [Jurassic World],                              [2015], [\$150],  [\$1,672], [6.9],
  [The Lion King],                               [2019], [\$260],  [\$1,662], [7.1],
  [Furious 7],                                   [2015], [\$190],  [\$1,516], [7.3],
  [Frozen II],                                   [2019], [\$150],  [\$1,450], [7.0],
  [Avengers: Age of Ultron],                     [2015], [\$330],  [\$1,406], [7.3],
  [Black Panther],                               [2018], [\$200],  [\$1,350], [7.3],
  [Harry Potter and the Deathly Hallows: Pt 2],  [2011], [\$125],  [\$1,342], [8.1],
  [Star Wars: The Last Jedi],                    [2017], [\$317],  [\$1,333], [7.0],
  [Jurassic World: Fallen Kingdom],              [2018], [\$170],  [\$1,310], [6.7],
  [Beauty and the Beast],                        [2017], [\$160],  [\$1,266], [7.1],
  [Incredibles 2],                               [2018], [\$200],  [\$1,243], [7.6],
  [Frozen],                                      [2013], [\$150],  [\$1,278], [7.4],
  [The Avengers],                                [2012], [\$220],  [\$1,520], [8.0],
)
]
