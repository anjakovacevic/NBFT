import math

class Analysis:
    """
    Implements the mathematical formulas from Section 4 of the NBFT paper.
    Used for plotting and theoretical verification.
    """
    
    @staticmethod
    def calculate_R(n: int, m: int) -> int:
        """
        Average nodes per group (excluding global primary if following (n-1) logic).
        Formula: R = floor((n-1)/m)
        """
        if m == 0: return 0
        return math.floor((n - 1) / m)

    @staticmethod
    def calculate_w(R: int) -> int:
        """
        Fault tolerance threshold within a group.
        Formula: w = floor((R-1)/3)
        """
        return math.floor((R - 1) / 3)

    @staticmethod
    def calculate_E(m: int) -> int:
        """
        Fault tolerance for the inter-group consensus (representatives).
        Formula: E = floor((m-1)/3)
        """
        return math.floor((m - 1) / 3)
    
    @staticmethod
    def combinations(n: int, k: int) -> int:
        return math.comb(n, k)

    @staticmethod
    def probability_fail(n: int, m: int, f: int) -> float:
        """
        Calculates the probability that the system fails.
        NBFT improves reliability. The paper likely provides a formula regarding 
        the probability of a group exceeding 'w' faulty nodes.
        
        A simplified approximation often used in these papers:
        P_fail ~ Sum(Prob(group_i fails))
        
        Using Hypergeometric distribution logic often found in sharding papers.
        (Visualizing the 'Fault Tolerance Interval' T mentioned in step 7).
        """
        # This is a placeholder for the specific Combinatorial formula C(i, R)/C(n, R) 
        # normally used to determine if a specific group gets too many bad nodes.
        # k = Number of byzantine nodes
        
        # P(X > w) where X is bad nodes in a group of size R drawn from pool n with f bad nodes.
        R = Analysis.calculate_R(n, m)
        w = Analysis.calculate_w(R)
        
        if R <= 0: return 1.0
        
        # Probability that a specific group has > w byzantine nodes
        # Hypergeometric distribution:
        # Population size: n
        # Success states in population: f (bad nodes)
        # Sample size: R
        # We fail if k_bad_in_sample > w
        
        total_fail_prob = 0.0
        
        # Sum prob for k = w+1 to min(f, R)
        for k in range(w + 1, min(f, R) + 1):
             # P(X=k) = C(f, k) * C(n-f, R-k) / C(n, R)
             numerator = math.comb(f, k) * math.comb(n - f, R - k)
             denominator = math.comb(n, R)
             if denominator > 0:
                 total_fail_prob += numerator / denominator
                 
        # This is for ONE group. Detailed system failure depends on consensus of groups.
        return total_fail_prob

    @staticmethod
    def communication_complexity_pbft(n: int) -> int:
        """
        Standard PBFT complexity is O(n^2).
        Approximation: 2n^2 (Prepare + Commit phases broadcast to all)
        """
        return 2 * (n * n)

    @staticmethod
    def communication_complexity_nbft(n: int, m: int) -> int:
        """
        NBFT Complexity.
        1. Global Primary -> m Representatives (m msgs)
        2. Intra-group: Rep -> (R-1) members (m * R)
        3. Intra-group vote: (R-1) members -> Rep (m * R)
        4. Inter-group: Reps -> Reps (PBFT among reps) -> O(m^2)
        
        Total approx: O(m^2 + n) instead of O(n^2) because n ≈ m*R
        """
        R = Analysis.calculate_R(n, m)
        intra_group = m * (2 * R) # Request + Response
        inter_group = 2 * (m * m) # PBFT among representatives
        
        return intra_group + inter_group
