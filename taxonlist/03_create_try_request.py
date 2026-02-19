import sys
from pathlib import Path
import pandas as pd
import re

def main():
    #
    # Select Trait IDs
    #

    # 1. Load the TRY trait list
    # We use skiprows=3 because the first three lines of the TRY export are metadata headers
    df = pd.read_csv('C:\drive_j\eigene\studium\Data_Science\Master-Thesis\Datasources\TRY\listoftraits_tde2026218232033.txt', sep='\t', skiprows=3)

    # 2. Define the strategic search terms (Regex patterns)
    # These terms capture mobility, physical adaptations, and release height
    keywords = [
        r'dispersal',    # Direct dispersal syndromes, agents, and distances
        r'diaspore',     # The actual dispersal unit (seed + appendages like wings/hooks)
        r'seed',         # Seed mass, seed terminal velocity, etc.
        r'height',       # Reproductive plant height (determines seed release trajectory)
        r'vector',       # Pollination or seed vectors
        r'longevity',    # Seed bank longevity (persistence in isolated patches)
        r'clonal'        # Clonal growth (survival without seed dispersal)
    ]

    # Combine into a single case-insensitive regular expression
    search_pattern = '|'.join(keywords)

    # Combine into a single Regex pattern using OR (|)
    search_pattern = '|'.join(keywords)

    # 4. Apply filter, handle NaNs, and extract matches
    # We use case=False to match "Seed" and "seed"
    fragmentation_traits = df[df['Trait'].fillna('').str.contains(search_pattern, case=False, regex=True)]

    # 5. Extract IDs, convert them to strings, and format as a comma-separated list
    trait_ids = fragmentation_traits['TraitID'].dropna().astype(int).astype(str).tolist()

    print("Selected TRY Trait IDs related to dispersal and mobility:")
    print(", ".join(trait_ids))

    # 
    # Select TRY Species IDs from merged taxon list
    #
    csv_path = Path(r"C:\drive_j\eigene\studium\Data_Science\Master-Thesis\Datasources\output\taxonlist\florkart-eiv-tryid.csv")
    if not csv_path.exists():
        print(f"File not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    df = pd.read_csv(csv_path, dtype=str)
    if 'TRY_SpeciesID' not in df.columns:
        print("Column 'TRY_SpeciesID' not found in CSV.", file=sys.stderr)
        sys.exit(1)

    # Output unique TRY Species IDs as a comma-separated list - full range (no filtering)
    ids = df['TRY_SpeciesID'].dropna().astype(str).str.strip()
    ids = ids[ids != '']
    unique_ids = pd.Series(ids.unique()).tolist()

    print("Selected unique TRY Species IDs - full range:")
    print(",".join(unique_ids))

    # Output unique TRY Species IDs as a comma-separated list - filtered for cnt_coord >= 10
    filtered_ids = df[pd.to_numeric(df['cnt_coord'], errors='coerce') >= 10]['TRY_SpeciesID'].dropna().astype(str).str.strip()
    filtered_ids = filtered_ids[filtered_ids != ''] 
    unique_filtered_ids = pd.Series(filtered_ids.unique()).tolist()

    print("Selected unique TRY Species IDs - filtered for cnt_coord >= 10:")
    print(",".join(unique_filtered_ids))

if __name__ == "__main__":
    main()