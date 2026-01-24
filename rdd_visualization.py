import numpy as np # type: ignore
import pandas as pd # type: ignore
import matplotlib.pyplot as plt # type: ignore

# =============================================================================
# RDD Visualization Demo:
#   Show (1) cutoff line and (2) bandwidth window |x| <= h
#
# Key idea in RDD:
#   - The causal effect is identified by comparing observations just below
#     and above a cutoff (threshold).
#   - We visualize a neighborhood around the cutoff: |x| <= h (note that this is choice variable)
# =============================================================================


# =============================================================================
# 1) Create a simulated Sharp RDD dataset
# =============================================================================

np.random.seed(7)         
n = 2000                   # sample size (number of observations)

# Running variable X (continuous)
# Think: test score / rating / income / eligibility index, etc.
# Here we simulate scores between 30 and 90.
X = np.random.uniform(30, 90, size=n)

# Cutoff in original scale
# Think: policy threshold (e.g., "below 60 must attend summer school")
c = 60

# Centered running variable:
#   x = X - c
# So the cutoff becomes x = 0 (this makes code + plots cleaner and easy to intepret).
x = X - c

# Sharp RDD treatment assignment:
# Treated if X >= cutoff  (equivalently, treated if x >= 0)
D = (x >= 0).astype(int)

# Outcome variable Y:
# We simulate an outcome that has:
#   - a smooth relationship with X (0.03 * X)
#   - a discontinuous jump at cutoff (0.8 * D)
#   - random noise (Normal(0, 0.25))
#
# That "0.8" is the true treatment effect in this toy example.
Y = 2 + 0.03 * X + 0.8 * D + np.random.normal(0, 0.25, size=n)

# Put everything in a DataFrame 
df = pd.DataFrame({
    "X": X,    # running variable (raw scale)
    "x": x,    # running variable (centered at cutoff)
    "D": D,    # treatment indicator
    "Y": Y     # outcome
})


# =============================================================================
# 2) Choose bandwidth window h (the local neighborhood around cutoff)
# =============================================================================

# Bandwidth h defines the neighborhood around cutoff:
#   |x| <= h
# Meaning: keep observations within h units of the cutoff for estimating the treatment effect 
#
# Example: if X is a score and cutoff is 60, then h=5 means:
#   keep scores in [55, 65]
h = 5

# Indicator: which observations are inside the bandwidth window?
# This allows us to highlight them in the plot.
in_bw = df["x"].abs() <= h


# =============================================================================
# 3) Compute binned means
# =============================================================================

# Raw scatter plots can look noisy. A common  trick is to show binned means:
# divide x into bins (intervals)
# compute mean Y within each bin
# plot these means as larger points
# This often helps reveal the discontinuity at cutoff. pd.cut is a function that bins a continuous variable into intervals
df["bin"] = pd.cut(df["x"], bins=30)

binned = (
    df.groupby("bin", observed=True)
      .agg(
          x_mean=("x", "mean"),   # average centered running variable in bin
          Y_mean=("Y", "mean")    # average outcome in bin
      )
      .reset_index(drop=True)
)


# =============================================================================
# 4) Plot: cutoff + bandwidth window + inside/outside points
# =============================================================================

plt.figure(figsize=(10, 6))

# Shade the bandwidth window |x| <= h; this highlights the "local comparison area" used in RDD.
plt.axvspan(
    -h, h,
    alpha=0.15,
    label=f"Bandwidth window |x| ≤ h (h={h})"
)

# Plot cutoff line (x=0); after centering, cutoff is always at 0.
plt.axvline(
    0,
    linestyle="--",
    label="Cutoff (x=0)"
)

# Scatter plot for observations outside bandwidth; we can make them faint (low alpha) so they fade into background.
plt.scatter(
    df.loc[~in_bw, "x"],
    df.loc[~in_bw, "Y"],
    s=12,
    alpha=0.20,
    label="Outside bandwidth"
)

# Scatter plot for observations inside bandwidth with higher alpha
plt.scatter(
    df.loc[in_bw, "x"],
    df.loc[in_bw, "Y"],
    s=12,
    alpha=0.55,
    label="Inside bandwidth"
)

# Plot binned means (large points)
# These provide a clean visual summary of the data pattern.
plt.scatter(
    binned["x_mean"],
    binned["Y_mean"],
    s=70,
    alpha=0.90,
    label="Binned means"
)

# Title + axis labels
plt.title("RDD Visualization: Cutoff and Bandwidth Window")
plt.xlabel("Centered running variable: x = X − cutoff")
plt.ylabel("Outcome Y")

# Legend 
plt.legend()

# tight layout 
plt.tight_layout()

# Display plot
plt.show()

from rdrobust import rdrobust # type: ignore
res = rdrobust(y=df["Y"].to_numpy(), x=df["x"].to_numpy(), c=0.0)
print(res)
