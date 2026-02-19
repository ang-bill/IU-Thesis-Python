import pandas as pd
import os

def main():
    csv_file = r"C:\drive_j\eigene\studium\Data_Science\Master-Thesis\Datasources\FlorKart\taxonlist-min300.csv"
    xlsx_file = r"C:\drive_j\eigene\studium\Data_Science\Master-Thesis\Datasources\EIV\stapfia-2024-0001_sm.xlsx"
    try_file = r"C:\drive_j\eigene\studium\Data_Science\Master-Thesis\Datasources\TRY\TryAccSpecies.txt"

    # --- Load and summarize CSV ---
    print("=== Processing taxonlist-min300.csv ===")
    df_taxa = pd.read_csv(csv_file)
    print(f"Total unique taxa: {df_taxa['taxon'].nunique()}")
    print("\nDescriptive Statistics (cnt):")
    print(df_taxa['cnt'].describe())

    print("\n" + "=" * 40 + "\n")

    # --- Load EIV data (no filtering yet — filter after merge) ---
    print("=== Processing stapfia-2024-0001_sm.xlsx ===")
    df_eiv = pd.read_excel(xlsx_file)

    # Ensure numeric types for filtering columns
    for col in ['L', 'F', 'N']:
        df_eiv[col] = pd.to_numeric(df_eiv[col], errors='coerce')

    # Derive binomial name (first 2 words) from EIV Taxon for fuzzy matching
    df_eiv['Taxon_binomial'] = df_eiv['Taxon'].str.split().str[:2].str.join(' ')

    print("\n" + "=" * 40 + "\n")

    # --- Two-pass merge to maximise match rate ---
    # taxonlist uses column 'taxon', EIV uses column 'Taxon'
    # Always use binomial (first 2 words) from df_taxa side.
    # Pass 1: match taxon_binomial against full EIV Taxon name (exact nomenclature)
    # Pass 2: for remaining unmatched rows, match taxon_binomial against EIV Taxon_binomial
    print("=== Joining taxonlist with EIV data (two-pass) ===")
    df_taxa['taxon_binomial'] = df_taxa['taxon'].str.split().str[:2].str.join(' ')

    # Pass 1 — exact match on full Taxon
    merged_pass1 = pd.merge(
        df_taxa,
        df_eiv,
        left_on='taxon_binomial',
        right_on='Taxon',
        how='inner'
    )
    print(f"  Pass 1 (taxon_binomial == Taxon): {len(merged_pass1)} matches")

    # Pass 2 — binomial fallback for rows not yet matched
    matched_binomials = merged_pass1['taxon_binomial'].unique()
    df_taxa_unmatched = df_taxa[~df_taxa['taxon_binomial'].isin(matched_binomials)]

    merged_pass2 = pd.merge(
        df_taxa_unmatched,
        df_eiv,
        left_on='taxon_binomial',
        right_on='Taxon_binomial',
        how='inner'
    )
    print(f"  Pass 2 (taxon_binomial == Taxon_binomial): {len(merged_pass2)} additional matches")

    # Combine both passes
    df_merged = pd.concat([merged_pass1, merged_pass2], ignore_index=True)
    print(f"  Total matches after two passes: {len(df_merged)}")

    # Rename EIV Taxon column to avoid confusion with taxonlist 'taxon' column
    if 'Taxon' in df_merged.columns:
        df_merged = df_merged.rename(columns={'Taxon': 'EIV_Taxon'})

    # Drop helper columns
    df_merged = df_merged.drop(columns=[c for c in ['taxon_binomial', 'Taxon_binomial'] if c in df_merged.columns])

    # --- Filter merged result by EIV indicator thresholds ---
    df_merged = df_merged[
        (df_merged['L'] >= 7) &  # Light preference (7-9 = high light) - from "Halblicht" to "Volllicht"
        (df_merged['F'] <= 4) &  # Moisture preference (1-4 = dry) - from "Trocken" to "Frisch"
 #       (df_merged['N'] >= 3)
        (df_merged['N'] <= 3)  # Nutrient preference (1-3 = low) - from "Sehr nährstoffarm" to "Nährstoffarm"
    ]
    print(f"  Rows after EIV filtering (L>=7, F<=4, N<=3): {len(df_merged)}")

    print("\n" + "=" * 40 + "\n")

    # --- Load TRY species list ---
    print("=== Joining with TRY species data (two-pass) ===")
    df_try = pd.read_csv(try_file, sep='\t', encoding='utf-8', usecols=['AccSpeciesID', 'AccSpeciesName'])

    # Derive binomial name (first 2 words) from TRY AccSpeciesName for fuzzy matching
    df_try['AccSpeciesName_binomial'] = df_try['AccSpeciesName'].str.split().str[:2].str.join(' ')

    # Rebuild a binomial key on the merged dataset for TRY matching
    df_merged['taxon_binomial'] = df_merged['taxon'].str.split().str[:2].str.join(' ')

    # TRY Pass 1: match taxon_binomial against full AccSpeciesName
    try_pass1 = pd.merge(
        df_merged,
        df_try[['AccSpeciesID', 'AccSpeciesName']],
        left_on='taxon_binomial',
        right_on='AccSpeciesName',
        how='inner'
    )
    print(f"  TRY Pass 1 (taxon_binomial == AccSpeciesName): {len(try_pass1)} matches")

    # TRY Pass 2: binomial fallback for rows not yet matched
    try_matched_binomials = try_pass1['taxon_binomial'].unique()
    df_merged_unmatched = df_merged[~df_merged['taxon_binomial'].isin(try_matched_binomials)]

    try_pass2 = pd.merge(
        df_merged_unmatched,
        df_try[['AccSpeciesID', 'AccSpeciesName', 'AccSpeciesName_binomial']],
        left_on='taxon_binomial',
        right_on='AccSpeciesName_binomial',
        how='inner'
    )
    print(f"  TRY Pass 2 (taxon_binomial == AccSpeciesName_binomial): {len(try_pass2)} additional matches")

    # TRY Pass 3 — POWO synonym mapping for rows still unmatched
    # Mapping: Florkart binomial name → TRY accepted species name
    # (based on POWO synonymy; empty TRY name means no TRY record exists)
    POWO_SYNONYM_MAP = {
        'Betonica alopecuros':  'Stachys alopecuros',
        'Betonica officinalis':  'Stachys officinalis',
        'Microrrhinum minus':    'Chaenorhinum minus',   # "s.str." stripped by binomial logic
        'Festuca pumila':        'Festuca quadriflora',
        'Melilotus albus':       'Trigonella alba',
        'Melilotus officinalis': 'Trigonella officinalis',
        'Hylotelephium maximum': 'Hylotelephium telephium subsp. maximum', # Sedum maximum 
        'Senecio jacobaea':      'Jacobaea vulgaris',
        'Chlorocrepis staticifolia': 'Tolpis staticifolia',
        'Jovibarba globifera s.lat.': 'Sempervivum globiferum', 
        'Jovibarba globifera': 'Sempervivum globiferum', 
        'Erigeron glabratus': 'Erigeron acris subsp. acris' ,
    }

    try_matched_binomials_p12 = pd.concat([
        try_pass1[['taxon_binomial']],
        try_pass2[['taxon_binomial']]
    ])['taxon_binomial'].unique()
    df_merged_unmatched2 = df_merged[~df_merged['taxon_binomial'].isin(try_matched_binomials_p12)].copy()

    # Apply synonym mapping: add a column with the TRY lookup name
    df_merged_unmatched2['_try_synonym'] = df_merged_unmatched2['taxon_binomial'].map(POWO_SYNONYM_MAP)

    # Sub-set that has a non-empty synonym → look those up in TRY
    has_synonym = df_merged_unmatched2['_try_synonym'].notna() & (df_merged_unmatched2['_try_synonym'] != '')
    df_with_synonym    = df_merged_unmatched2[has_synonym].copy()
    df_without_synonym = df_merged_unmatched2[~has_synonym].copy()

    # Match synonym name against full TRY AccSpeciesName
    try_pass3 = pd.merge(
        df_with_synonym,
        df_try[['AccSpeciesID', 'AccSpeciesName']],
        left_on='_try_synonym',
        right_on='AccSpeciesName',
        how='inner'
    )
    # Fallback: match synonym binomial against TRY AccSpeciesName_binomial
    try_pass3_matched = try_pass3['_try_synonym'].unique()
    df_with_synonym_unmatched = df_with_synonym[~df_with_synonym['_try_synonym'].isin(try_pass3_matched)]
    try_pass3b = pd.merge(
        df_with_synonym_unmatched,
        df_try[['AccSpeciesID', 'AccSpeciesName', 'AccSpeciesName_binomial']],
        left_on='_try_synonym',
        right_on='AccSpeciesName_binomial',
        how='inner'
    )
    try_pass3b = try_pass3b.drop(columns=['AccSpeciesName_binomial'], errors='ignore')

    synonym_matched = len(try_pass3) + len(try_pass3b)
    print(f"  TRY Pass 3 (POWO synonym mapping):             {synonym_matched} additional matches")

    # Clean up helper column from all synonym dataframes
    try_pass3  = try_pass3.drop(columns=['_try_synonym'], errors='ignore')
    try_pass3b = try_pass3b.drop(columns=['_try_synonym'], errors='ignore')

    # Rows with no TRY match at all — keep them but fill TRY columns with NaN
    all_try_matched_binomials = pd.concat([
        try_pass1[['taxon_binomial']],
        try_pass2[['taxon_binomial']],
        try_pass3[['taxon_binomial']],
        try_pass3b[['taxon_binomial']]
    ])['taxon_binomial'].unique()

    df_merged_no_try = df_merged[~df_merged['taxon_binomial'].isin(all_try_matched_binomials)].copy()
    df_merged_no_try['AccSpeciesID'] = pd.NA
    df_merged_no_try['AccSpeciesName'] = pd.NA
    # Also drop the synonym helper column if it was carried over
    df_merged_no_try = df_merged_no_try.drop(columns=['_try_synonym'], errors='ignore')

    # Combine all four parts
    df_merged = pd.concat(
        [try_pass1, try_pass2, try_pass3, try_pass3b, df_merged_no_try],
        ignore_index=True
    )
    print(f"  Total rows after TRY join (including unmatched): {len(df_merged)}")
    try_matched_count = df_merged['AccSpeciesID'].notna().sum()
    print(f"  Rows with TRY match: {try_matched_count}")

    # Drop temporary helper columns
    df_merged = df_merged.drop(
        columns=[c for c in ['taxon_binomial', 'AccSpeciesName_binomial'] if c in df_merged.columns]
    )

    # Rename TRY columns for clarity
    df_merged = df_merged.rename(columns={
        'AccSpeciesID': 'TRY_SpeciesID',
        'AccSpeciesName': 'TRY_SpeciesName'
    })

    print(f"\nColumns in merged dataframe: {df_merged.columns.tolist()}")
    print("\nFirst rows of merged data:")
    print(df_merged.head())

    # --- Write output files ---
    output_dir = r"C:\drive_j\eigene\studium\Data_Science\Master-Thesis\Datasources\output\taxonlist"
    os.makedirs(output_dir, exist_ok=True)

    csv_out = os.path.join(output_dir, "florkart-eiv-tryid.csv")
    xlsx_out = os.path.join(output_dir, "florkart-eiv-tryid.xlsx")

    df_merged.to_csv(csv_out, index=False)
    df_merged.to_excel(xlsx_out, index=False)

    print(f"\nOutput written to:")
    print(f"  CSV:  {csv_out}")
    print(f"  XLSX: {xlsx_out}")


if __name__ == "__main__":
    main()
