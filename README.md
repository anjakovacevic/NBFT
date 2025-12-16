# NBFT Educational Simulation Platform

This project recreates the simulations from the research paper **"Improved Fault-Tolerant Consensus Based on the PBFT Algorithm"** (NBFT). It provides an interactive playground to understand how grouping nodes reduces communication complexity from O(n²) to O(n) and improves fault tolerance.

## Project Structure & Paper Mapping

The codebase is modularized to map directly to sections of the research paper:

### Core Logic (`nbft/`)
*   **`models.py`**: Defines standard data structures like `Node`, `Message`, and `Vote`.
    *   *Maps to:* General System Model.
*   **`consistent_hash.py`**: Implements the hash ring and group formation logic.
    *   *Maps to:* **Section 3.1 (Improved Consensus Node Grouping)**. Computes $primary = H_{ring}(v)$ and groups nodes based on virtual IDs.
*   **`pbft_sim.py`**: A simplified PBFT implementation used as a baseline.
    *   *Maps to:* **Section 2 (PBFT Algorithm)**. Used to reproduce the O(n²) communication curves in the comparative analysis.
*   **`nbft_sim.py`**: The core NBFT consensus algorithm implementation.
    *   *Maps to:* **Section 3.2 (NBFT Algorithm)**. Implements Algorithm 1 (Node Decision) and Algorithm 2 (Vote-Counting) with the intra-group and inter-group phases.
*   **`byzantine.py`**: Defines malicious node behaviors.
    *   *Maps to:* **Section 4.1 (Experimental Setup)**. Models non-primary flaws, data tampering, and silent failures to test robustness.
*   **`analysis.py`**: Contains the mathematical formulas for fault tolerance and complexity.
    *   *Maps to:* **Section 4.2 (Fault Tolerance Analysis)** and **Section 4.3 (Communication Complexity)**. Calculates $P_{fail}$, $R$, $E$, and $w$.

### Infrastructure
*   **`app.py`**: The Gradio web interface entry point. Allows students to visualize the algorithms.
    *   *Educational Goal:* Interactive learning and parameter sweeping.
*   **`db/`**: SQLite database helpers to save and load experiment runs.
    *   *Educational Goal:* Reproducibility of experiments.
*   **`experiments/`**: Batch runners for generating the data used in the paper's plots.
    *   *Maps to:* **Section 5 (Performance Analysis)**. Recreates Figures 4, 5, and 6.

## Getting Started

1.  So far, the structure is created. Run `app.py` after installation (instructions TBD).
