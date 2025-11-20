"""
Workload script management with weighted random selection.

This module provides classes for managing multiple SQL workload scripts
with configurable weights for random selection during benchmark execution.
"""

import random
from typing import List, Tuple


class WorkloadScript:
    """
    Represents a single SQL workload script with its weight.

    Attributes:
        filepath: Original file path (for error messages/logging)
        raw_content: Original SQL content from file
        formatted_content: SQL with {table_folder} placeholder replaced
        weight: Numeric weight for random selection (default: 1.0)
        uses_bid: Whether script uses $bid parameter
        uses_tid: Whether script uses $tid parameter
        uses_aid: Whether script uses $aid parameter
        uses_delta: Whether script uses $delta parameter
        uses_iteration: Whether script uses $iteration parameter
    """

    def __init__(self, filepath: str, content: str, weight: float, table_folder: str):
        """
        Initialize a workload script.

        Args:
            filepath: Path to the SQL file
            content: Raw SQL content
            weight: Weight for random selection (must be > 0)
            table_folder: Folder name to replace {table_folder} placeholder

        Raises:
            ValueError: If weight is not positive
        """
        if weight <= 0:
            raise ValueError(f"Weight must be positive, got {weight} for {filepath}")

        self.filepath = filepath
        self.raw_content = content
        self.weight = weight

        # Format the script with table_folder (same as current Job.__init__)
        self.formatted_content = content.format(table_folder=table_folder)

        # Detect which parameters are used (cached for performance)
        self.uses_bid = "$bid" in self.formatted_content
        self.uses_tid = "$tid" in self.formatted_content
        self.uses_aid = "$aid" in self.formatted_content
        self.uses_delta = "$delta" in self.formatted_content
        self.uses_iteration = "$iteration" in self.formatted_content


class WeightedScriptSelector:
    """
    Manages multiple workload scripts and provides weighted random selection.

    Uses a simple linear scan approach suitable for small numbers of scripts (<10).
    """

    def __init__(self, scripts: List[WorkloadScript]):
        """
        Initialize selector with a list of workload scripts.

        Args:
            scripts: List of WorkloadScript objects

        Raises:
            ValueError: If scripts list is empty or total weight is <= 0
        """
        if not scripts:
            raise ValueError("At least one script must be provided")

        self.scripts = scripts

        # Calculate total weight for normalization
        self.total_weight = sum(script.weight for script in scripts)

        if self.total_weight <= 0:
            raise ValueError("Total weight must be positive")

        # Build cumulative weights for selection
        # Example: weights [70, 20, 10] -> cumulative [70, 90, 100]
        self.cumulative_weights = []
        cumulative = 0.0
        for script in scripts:
            cumulative += script.weight
            self.cumulative_weights.append(cumulative)

    def select_random(self) -> WorkloadScript:
        """
        Select a random script based on weights.

        Returns:
            Selected WorkloadScript object

        Algorithm:
            1. Generate random value in [0, total_weight)
            2. Linear scan to find which script's range contains the value
            3. Return that script

        Example with weights [70, 20, 10]:
            - Random value 35 -> first script (0-70)
            - Random value 85 -> second script (70-90)
            - Random value 95 -> third script (90-100)
        """
        # Generate random value in range [0, total_weight)
        rand_value = random.uniform(0, self.total_weight)

        # Linear scan to find which script (simple approach for <10 scripts)
        for i, cumulative in enumerate(self.cumulative_weights):
            if rand_value < cumulative:
                return self.scripts[i]

        # Fallback (should never happen due to floating point)
        return self.scripts[-1]

    def get_script_with_params(self) -> Tuple[str, WorkloadScript]:
        """
        Select script and return both content and metadata.

        Returns:
            Tuple of (formatted_content, WorkloadScript object)

        This allows Job to access parameter detection flags without
        re-parsing the script.
        """
        script = self.select_random()
        return script.formatted_content, script
