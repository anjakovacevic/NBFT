import matplotlib.pyplot as plt
import numpy as np
from nbft.consistent_hash import ConsistentHashing, RING_SIZE, sha32
from nbft.models import Node
import random

def visualize_consistent_hashing(n=10, m=3, view_number=0):
    # 1. Setup Nodes
    nodes = [Node(i, f"pub_{i}") for i in range(n)]
    ch = ConsistentHashing(nodes, m)
    
    # 2. Run Hashing Logic
    # We use a dummy previous hash for visualization
    prev_hash = "0" * 64
    groups, node_group_map, global_primary_id = ch.form_groups(view_number, prev_hash)
    reps = {g.representative_id for g in groups}
    
    # 3. Visualization Setup
    fig, ax = plt.subplots(figsize=(10, 10), subplot_kw={'projection': 'polar'})
    ax.set_theta_offset(np.pi/2) # Start 0 at top
    ax.set_theta_direction(-1)   # Clockwise
    
    # Draw the Ring (Unit Circle)
    theta_ring = np.linspace(0, 2*np.pi, 200)
    ax.plot(theta_ring, [1.0]*200, color='gray', linestyle='--', alpha=0.5)
    
    # Create a colormap for groups
    cmap = plt.get_cmap('tab10')
    
    # 4. Plot Nodes
    for node_id, pos in ch.pos_by_id.items():
        theta = (pos / RING_SIZE) * 2 * np.pi
        gid = node_group_map.get(node_id, -1)
        
        # Color: Primary is blue, others are based on group
        if node_id == global_primary_id:
            color = 'blue'
            size = 200
            label = f"PRI (N{node_id})"
            zorder = 5
        elif gid == -1: # Remainder or unassigned
            color = 'gray'
            size = 50
            label = f"N{node_id}"
            zorder = 2
        elif node_id in reps:
            color = cmap(gid % 10)
            size = 120
            label = f"REP (N{node_id}, G{gid})"
            zorder = 4
        else:
            color = cmap(gid % 10)
            size = 60
            label = f"N{node_id} (G{gid})"
            zorder = 3
            
        ax.scatter(theta, 1.0, color=color, s=size, zorder=zorder)
        # Add a subtle edge to Reps to distinguish them further
        if node_id in reps and node_id != global_primary_id:
            ax.scatter(theta, 1.0, facecolors='none', edgecolors='black', s=size+40, lw=1.5, zorder=4.1)
            
        ax.text(theta, 1.15, label, ha='center', va='center', fontsize=8, fontweight='bold')
        
    # 5. Fix Limits & Circularity
    ax.set_ylim(0, 1.3) # Give room for labels outside the r=1.0 ring
    ax.set_aspect('equal') # Ensure it's a perfect circle


    # 6. Styling
    ax.set_rticks([]) # Hide radial ticks
    ax.set_yticklabels([])
    ax.set_title(f"NBFT Consistent Hashing Ring (v={view_number}, n={n}, m={m})", pad=20)
    
    # Add a custom legend
    from matplotlib.lines import Line2D
    legend_elements = [
        Line2D([0], [0], marker='o', color='w', label='Global Primary', markerfacecolor='blue', markersize=12),
        Line2D([0], [0], marker='o', color='w', label='Representative', markerfacecolor='green', markersize=10),
        Line2D([0], [0], marker='o', color='w', label='Node', markerfacecolor='black', markersize=7),
    ]
    ax.legend(handles=legend_elements, loc='upper right', bbox_to_anchor=(1.2, 1.1))

    # Use subplots_adjust instead of tight_layout to prevent the warning
    # Polar plots often struggle with tight_layout when labels are outside the axes
    fig.subplots_adjust(top=0.85, bottom=0.05, left=0.05, right=0.85)
    plt.show()

if __name__ == "__main__":
    visualize_consistent_hashing(n=20, m=4)
