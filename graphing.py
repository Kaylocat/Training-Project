import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd
import numpy as np

def plot_genres_bar(df: pd.DataFrame, outpath: Path) -> None:   
    fig, ax = plt.subplots(figsize=(9, 8))
    y_pos = np.arange(len(df["genre_name"]))

    hbars = ax.barh(y_pos, df["avg_score"], align='center')
    ax.set_yticks(y_pos, df["genre_name"])
    ax.invert_yaxis()

    ax.set_xlabel('Average Score')
    ax.set_xlim(0, 10)
    ax.set_title('Scores By Genre')
    ax.bar_label(hbars, fmt='{:,.3f}', padding=5)

    fig.tight_layout()
    fig.savefig(outpath)

def plot_year_bar(df: pd.DataFrame, outpath: Path) -> None:
    fig, ax = plt.subplots(figsize=(12, 8))
    plt.figure(figsize=(8, 5))
    ax.scatter(df["year"], df["avg_score"])
    ax.set_title("Distribution of Scores by Year")
    ax.set_xlabel("Year")
    ax.set_ylabel("Average Score")

    m, b = np.polyfit(df["year"], df["avg_score"], 1)
    ax.plot(df["year"], m*df["year"] + b, color='steelblue', linestyle='--', linewidth=2, label=f'Line of Fit (y = {m:.2f}x + {b:.2f})')
    fig.show()
    fig.tight_layout()
    fig.savefig(outpath)
