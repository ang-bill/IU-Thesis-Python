import pandas as pd
import sqlalchemy
import os

SQL = """
WITH filtered_taxa AS (
    SELECT idtaxon, fullname, cnt_taxon_fs_r9_oa, lft, rgt
    FROM fm_diversity.vw_alphadiversity_overview_taxa_territoryparent
    WHERE idtaxgrade = 'spec'
      AND idterritoryparent = 'A'
      AND cnt_taxon_fs_r9_oa >= 300
)
SELECT a.idtaxon, a.fullname taxon, a.cnt_taxon_fs_r9_oa cnt, a.lft, a.rgt,
       COUNT(s.lft) cnt_coord
FROM filtered_taxa a
LEFT OUTER JOIN fm_core.vw_sample s
    ON s.lft BETWEEN a.lft AND a.rgt
   AND s.rgt BETWEEN a.lft AND a.rgt
   AND s.coord_lat_dms IS NOT NULL
   AND s.coord_long_dms IS NOT NULL
GROUP BY a.idtaxon, a.fullname, a.cnt_taxon_fs_r9_oa, a.lft, a.rgt
"""

OUTPUT_DIR = r"C:\drive_j\eigene\studium\Data_Science\Master-Thesis\Datasources\FlorKart"
CSV_OUT  = os.path.join(OUTPUT_DIR, "taxonlist-min300.csv")
XLSX_OUT = os.path.join(OUTPUT_DIR, "taxonlist-min300.xlsx")


def main():
    # --- Connect to database ---
    engine = sqlalchemy.create_engine(
        "mysql+pymysql://fm_web_read:Flora17PlantBio@127.0.0.1:3306/fm_core"
    )

    print("=== Querying FlorKart database ===")
    with engine.connect() as conn:
        df = pd.read_sql(SQL, conn)

    print(f"Rows returned: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    print("\nFirst rows:")
    print(df.head())

    print("\nDescriptive statistics (cnt):")
    print(df["cnt"].describe())

    # --- Write output files ---
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    df.to_csv(CSV_OUT, index=False)
    print(f"\nCSV  written: {CSV_OUT}")

    df.to_excel(XLSX_OUT, index=False)
    print(f"XLSX written: {XLSX_OUT}")


if __name__ == "__main__":
    main()
