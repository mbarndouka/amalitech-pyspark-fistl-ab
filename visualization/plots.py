"""
Movie data visualizations — matplotlib + numpy, no pandas required.

Seaborn is skipped because it imports pandas at module load, which triggers a
Windows Application Control DLL block in this environment.  All styling that
would normally come from seaborn is replicated via matplotlib rcParams and
manual aesthetic choices driven by VizSettings in config.toml.
"""

import pathlib
from collections import defaultdict

# Non-interactive backend — must be set before pyplot is imported.
import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
import numpy as np
import pyarrow.parquet as pq

from config.settings import get_settings
from utils.logger import get_logger

logger = get_logger(__name__)


# ── Data loading ───────────────────────────────────────────────────────────────

def _load(df=None) -> dict:
    """Return movie data as a plain dict of Python lists.

    Args:
        df: Optional cleaned Spark DataFrame. When provided it is collected
            and converted via PyArrow (same type handling as the parquet path)
            so no intermediate file write is required.
            When None the cleaned parquet on disk is read directly.
    """
    if df is not None:
        from utils.parquet import spark_df_to_dict
        return spark_df_to_dict(df)
    from config.settings import get_settings
    path = get_settings().storage.processed_data_path / "movies_cleaned.parquet"
    return pq.read_table(path).to_pydict()


# ── Theme ──────────────────────────────────────────────────────────────────────

def _apply_theme(v) -> None:
    """Push VizSettings into matplotlib's global rcParams."""
    plt.rcParams.update({
        "figure.facecolor":   v.background_color,
        "axes.facecolor":     v.surface_color,
        "axes.edgecolor":     v.grid_color,
        "axes.labelcolor":    v.text_color,
        "axes.titlecolor":    v.text_color,
        "axes.titlesize":     13,
        "axes.labelsize":     11,
        "axes.grid":          True,
        "grid.color":         v.grid_color,
        "grid.linestyle":     "--",
        "grid.alpha":         0.45,
        "xtick.color":        v.text_color,
        "ytick.color":        v.text_color,
        "xtick.labelsize":    9,
        "ytick.labelsize":    9,
        "legend.facecolor":   v.surface_color,
        "legend.edgecolor":   v.grid_color,
        "legend.labelcolor":  v.text_color,
        "legend.fontsize":    9,
        "figure.dpi":         v.fig_dpi,
        "savefig.facecolor":  v.background_color,
        "savefig.bbox":       "tight",
        "savefig.pad_inches": 0.2,
        "font.family":        "DejaVu Sans",
    })


def _save(fig, name: str, output_dir: pathlib.Path) -> pathlib.Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / name
    fig.savefig(path)
    plt.close(fig)
    logger.info(f"Saved → {path}")
    return path


# ── Helper: regression line ────────────────────────────────────────────────────

def _regline(x: np.ndarray, y: np.ndarray, x_range: np.ndarray) -> np.ndarray:
    coeffs = np.polyfit(x, y, 1)
    return np.polyval(coeffs, x_range)


# ── Chart 1: Revenue vs Budget ────────────────────────────────────────────────

def plot_revenue_vs_budget(data: dict, v, output_dir: pathlib.Path) -> pathlib.Path:
    """Scatter of Revenue vs Budget with break-even line and regression."""
    budgets  = np.array(data["budget_musd"],  dtype=float)
    revenues = np.array(data["revenue_musd"], dtype=float)
    years    = np.array([d.year for d in data["release_date"]], dtype=float)
    titles   = data["title"]

    valid = ~(np.isnan(budgets) | np.isnan(revenues))
    budgets, revenues, years, titles = (
        budgets[valid], revenues[valid], years[valid],
        [t for t, v_ in zip(titles, valid) if v_],
    )

    fig, ax = plt.subplots(figsize=(v.fig_width, v.fig_height))

    # Colour by year
    norm   = plt.Normalize(years.min(), years.max())
    cmap   = plt.cm.plasma
    colors = cmap(norm(years))

    sc = ax.scatter(budgets, revenues, c=colors, s=120, alpha=0.85,
                    edgecolors=v.grid_color, linewidths=0.6, zorder=3)

    # Break-even line (revenue = budget)
    lim = max(budgets.max(), revenues.max()) * 1.05
    ax.plot([0, lim], [0, lim], color=v.muted_color, lw=1.2,
            linestyle=":", label="Break-even (R = B)", zorder=2)

    # Regression line
    x_range = np.linspace(budgets.min(), budgets.max(), 200)
    y_reg   = _regline(budgets, revenues, x_range)
    ax.plot(x_range, y_reg, color=v.palette[0], lw=2,
            linestyle="--", label="Regression trend", zorder=2)

    # Annotate top-5 by revenue
    top5_idx = np.argsort(revenues)[-5:]
    for i in top5_idx:
        ax.annotate(
            titles[i],
            xy=(budgets[i], revenues[i]),
            xytext=(8, 4), textcoords="offset points",
            fontsize=7, color=v.text_color, alpha=0.9,
        )

    cbar = fig.colorbar(plt.cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax, pad=0.02)
    cbar.set_label("Release Year", color=v.text_color, fontsize=9)
    cbar.ax.yaxis.set_tick_params(color=v.text_color, labelcolor=v.text_color)

    ax.set_xlabel("Production Budget (M USD)")
    ax.set_ylabel("Box Office Revenue (M USD)")
    ax.set_title("Revenue vs Budget — with Break-even & Regression Trend")
    ax.legend(loc="upper left")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.0f}M"))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.0f}M"))

    return _save(fig, "01_revenue_vs_budget.png", output_dir)


# ── Chart 2: ROI Distribution by Genre ────────────────────────────────────────

def plot_roi_by_genre(data: dict, v, output_dir: pathlib.Path) -> pathlib.Path:
    """Horizontal bar chart of mean ROI per genre, annotated with movie count."""
    budgets  = data["budget_musd"]
    revenues = data["revenue_musd"]
    genres_list = data["genres"]

    # Compute ROI per movie
    roi_map: dict[str, list[float]] = defaultdict(list)
    for genre_str, bud, rev in zip(genres_list, budgets, revenues):
        if genre_str and bud and bud > 0 and rev is not None:
            roi = round(rev / bud, 4)
            for genre in genre_str.split("|"):
                genre = genre.strip()
                if genre:
                    roi_map[genre].append(roi)

    if not roi_map:
        logger.warning("No genre data — skipping ROI by Genre chart")
        return None

    genres_sorted = sorted(roi_map, key=lambda g: np.mean(roi_map[g]), reverse=True)
    means  = [np.mean(roi_map[g]) for g in genres_sorted]
    counts = [len(roi_map[g])    for g in genres_sorted]
    colors = [v.palette[i % len(v.palette)] for i in range(len(genres_sorted))]

    fig, ax = plt.subplots(figsize=(v.fig_width, max(6, len(genres_sorted) * 0.55)))

    bars = ax.barh(genres_sorted, means, color=colors, edgecolor=v.grid_color,
                   linewidth=0.5, alpha=0.88, height=0.65)

    # Annotate mean ROI and count on each bar
    for bar, mean_val, cnt in zip(bars, means, counts):
        ax.text(
            mean_val + 0.05, bar.get_y() + bar.get_height() / 2,
            f"{mean_val:.2f}×  (n={cnt})",
            va="center", ha="left", fontsize=8, color=v.text_color,
        )

    ax.set_xlabel("Mean ROI  (Revenue ÷ Budget)")
    ax.set_title("ROI Distribution by Genre")
    ax.axvline(1.0, color=v.muted_color, linestyle=":", lw=1.2, label="Break-even ROI = 1×")
    ax.legend(loc="lower right")
    ax.invert_yaxis()

    return _save(fig, "02_roi_by_genre.png", output_dir)


# ── Chart 3: Popularity vs Rating ─────────────────────────────────────────────

def plot_popularity_vs_rating(data: dict, v, output_dir: pathlib.Path) -> pathlib.Path:
    """Bubble scatter: x=vote_average, y=popularity, size=vote_count, colour=budget."""
    vote_avg   = np.array(data["vote_average"], dtype=float)
    popularity = np.array(data["popularity"],   dtype=float)
    vote_cnt   = np.array(data["vote_count"],   dtype=float)
    budgets    = np.array(data["budget_musd"],  dtype=float)
    titles     = data["title"]

    valid = ~(np.isnan(vote_avg) | np.isnan(popularity))
    vote_avg, popularity, vote_cnt, budgets, titles = (
        vote_avg[valid], popularity[valid], vote_cnt[valid],
        budgets[valid], [t for t, v_ in zip(titles, valid) if v_],
    )

    sizes = (vote_cnt / vote_cnt.max()) * 400 + 40

    norm   = plt.Normalize(budgets.min(), budgets.max())
    cmap   = plt.cm.YlOrRd
    colors = cmap(norm(budgets))

    fig, ax = plt.subplots(figsize=(v.fig_width, v.fig_height))

    sc = ax.scatter(vote_avg, popularity, s=sizes, c=colors, alpha=0.80,
                    edgecolors=v.grid_color, linewidths=0.5, zorder=3)

    # Annotate top-6 by popularity
    top6 = np.argsort(popularity)[-6:]
    for i in top6:
        ax.annotate(
            titles[i],
            xy=(vote_avg[i], popularity[i]),
            xytext=(6, 3), textcoords="offset points",
            fontsize=7.5, color=v.text_color, alpha=0.9,
        )

    cbar = fig.colorbar(plt.cm.ScalarMappable(norm=norm, cmap=cmap), ax=ax, pad=0.02)
    cbar.set_label("Budget (M USD)", color=v.text_color, fontsize=9)
    cbar.ax.yaxis.set_tick_params(color=v.text_color, labelcolor=v.text_color)

    # Bubble-size legend
    for size_val, label in [(10_000, "10k votes"), (30_000, "30k votes")]:
        ax.scatter([], [], s=(size_val / vote_cnt.max()) * 400 + 40,
                   color=v.muted_color, alpha=0.6, label=label)
    ax.legend(loc="upper left", title="Vote count", title_fontsize=8)

    ax.set_xlabel("Average Rating  (vote_average)")
    ax.set_ylabel("Popularity Score")
    ax.set_title("Popularity vs Rating  —  bubble size = vote count, colour = budget")

    return _save(fig, "03_popularity_vs_rating.png", output_dir)


# ── Chart 4: Yearly Box Office Trends ─────────────────────────────────────────

def plot_yearly_trends(data: dict, v, output_dir: pathlib.Path) -> pathlib.Path:
    """Grouped bars (budget vs revenue) with a profit overlay line per year."""
    years    = [d.year for d in data["release_date"]]
    budgets  = data["budget_musd"]
    revenues = data["revenue_musd"]

    # Aggregate by year
    year_budget:  dict[int, list[float]] = defaultdict(list)
    year_revenue: dict[int, list[float]] = defaultdict(list)
    for yr, b, r in zip(years, budgets, revenues):
        if b is not None and r is not None:
            year_budget[yr].append(b)
            year_revenue[yr].append(r)

    sorted_years = sorted(year_budget)
    mean_bud = np.array([np.mean(year_budget[y])  for y in sorted_years])
    mean_rev = np.array([np.mean(year_revenue[y]) for y in sorted_years])
    profit   = mean_rev - mean_bud
    x        = np.arange(len(sorted_years))
    width    = 0.35

    fig, ax1 = plt.subplots(figsize=(v.fig_width, v.fig_height))
    ax2 = ax1.twinx()

    b1 = ax1.bar(x - width / 2, mean_bud, width, label="Mean Budget",
                 color=v.palette[1], alpha=0.80, edgecolor=v.grid_color, linewidth=0.4)
    b2 = ax1.bar(x + width / 2, mean_rev, width, label="Mean Revenue",
                 color=v.palette[0], alpha=0.80, edgecolor=v.grid_color, linewidth=0.4)

    # Profit line on secondary axis
    ax2.plot(x, profit, color=v.palette[2], lw=2.2, marker="o",
             markersize=6, zorder=4, label="Mean Profit")
    ax2.axhline(0, color=v.muted_color, linestyle=":", lw=1)
    ax2.set_ylabel("Mean Profit (M USD)", color=v.palette[2], fontsize=11)
    ax2.tick_params(axis="y", colors=v.palette[2])

    ax1.set_xticks(x)
    ax1.set_xticklabels(sorted_years, rotation=45, ha="right")
    ax1.set_xlabel("Release Year")
    ax1.set_ylabel("Mean Value (M USD)")
    ax1.set_title("Yearly Box Office Trends — Budget, Revenue & Profit")
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.0f}M"))
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x:.0f}M"))

    # Combined legend
    handles = [b1, b2,
               plt.Line2D([0], [0], color=v.palette[2], lw=2.2, marker="o", markersize=6,
                          label="Mean Profit")]
    ax1.legend(handles=handles, loc="upper left")

    return _save(fig, "04_yearly_trends.png", output_dir)


# ── Chart 5: Franchise vs Standalone ──────────────────────────────────────────

def plot_franchise_vs_standalone(data: dict, v, output_dir: pathlib.Path) -> pathlib.Path:
    """Grouped bar chart comparing Franchise vs Standalone across 4 KPIs."""
    labels     = ["Franchise", "Standalone"]
    colors     = [v.franchise_color, v.standalone_color]
    metrics    = ["Mean Revenue\n(M USD)", "Mean Budget\n(M USD)",
                  "Mean Popularity", "Mean Rating"]

    # Classify each movie
    groups: dict[str, dict[str, list]] = {
        "Franchise":  defaultdict(list),
        "Standalone": defaultdict(list),
    }
    for i, coll in enumerate(data["belongs_to_collection"]):
        group = "Franchise" if (coll and str(coll).strip()) else "Standalone"
        groups[group]["revenue"].append(data["revenue_musd"][i]  or 0)
        groups[group]["budget"].append(data["budget_musd"][i]    or 0)
        groups[group]["popularity"].append(data["popularity"][i] or 0)
        groups[group]["rating"].append(data["vote_average"][i]   or 0)

    def safe_mean(lst):
        return float(np.mean(lst)) if lst else 0.0

    franchise_vals  = [safe_mean(groups["Franchise"]["revenue"]),
                       safe_mean(groups["Franchise"]["budget"]),
                       safe_mean(groups["Franchise"]["popularity"]),
                       safe_mean(groups["Franchise"]["rating"])]
    standalone_vals = [safe_mean(groups["Standalone"]["revenue"]),
                       safe_mean(groups["Standalone"]["budget"]),
                       safe_mean(groups["Standalone"]["popularity"]),
                       safe_mean(groups["Standalone"]["rating"])]

    n_metrics = len(metrics)
    x         = np.arange(n_metrics)
    width     = 0.32

    fig, ax = plt.subplots(figsize=(v.fig_width, v.fig_height))

    bars_f = ax.bar(x - width / 2, franchise_vals,  width, label=f"Franchise  (n={len(groups['Franchise']['revenue'])})",
                    color=v.franchise_color,  alpha=0.85, edgecolor=v.grid_color, linewidth=0.5)
    bars_s = ax.bar(x + width / 2, standalone_vals, width, label=f"Standalone (n={len(groups['Standalone']['revenue'])})",
                    color=v.standalone_color, alpha=0.85, edgecolor=v.grid_color, linewidth=0.5)

    # Value labels on bars
    for bar in list(bars_f) + list(bars_s):
        h = bar.get_height()
        ax.text(bar.get_x() + bar.get_width() / 2, h + h * 0.02,
                f"{h:.1f}", ha="center", va="bottom", fontsize=8, color=v.text_color)

    ax.set_xticks(x)
    ax.set_xticklabels(metrics)
    ax.set_title("Franchise vs Standalone — Mean Performance Across Key Metrics")
    ax.set_ylabel("Value (metric-dependent — see x-axis labels)")
    ax.legend()

    # Note if data is partial
    if not groups["Franchise"]["revenue"]:
        ax.text(0.5, 0.5, "No franchise data in current dataset\n(API collection names not yet populated)",
                transform=ax.transAxes, ha="center", va="center",
                fontsize=11, color=v.muted_color,
                bbox=dict(boxstyle="round,pad=0.5", facecolor=v.surface_color, alpha=0.8))

    return _save(fig, "05_franchise_vs_standalone.png", output_dir)


# ── Pipeline entry point ───────────────────────────────────────────────────────

def run_visualization(df=None):
    """Generate all 5 charts and save them to the configured output directory.

    Args:
        df: Optional cleaned Spark DataFrame from run_cleaning(). When provided
            data is read directly from the DataFrame (no parquet file needed).
            When None the cleaned parquet on disk is read via PyArrow.
    """
    logger.info("Starting visualization pipeline")
    settings = get_settings()
    v = settings.visualization

    _apply_theme(v)

    output_dir = pathlib.Path(v.output_dir)
    data = _load(df)
    logger.info(f"Loaded {len(data['title'])} movies for visualization")

    plot_revenue_vs_budget(data, v, output_dir)
    plot_roi_by_genre(data, v, output_dir)
    plot_popularity_vs_rating(data, v, output_dir)
    plot_yearly_trends(data, v, output_dir)
    plot_franchise_vs_standalone(data, v, output_dir)

    logger.info(f"All plots saved to: {output_dir}")
