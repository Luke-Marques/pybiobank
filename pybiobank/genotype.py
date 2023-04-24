import polars as pl
from pathlib import Path
from itertools import chain


def read_fam_file(ukb_project_dir: Path) -> pl.DataFrame:
    """_summary_

    Args:
        ukb_project_dir (Path): _description_

    Returns:
        pl.DataFrame: _description_
    """
    
    genotyped_subdir = ukb_project_dir / 'genotyped'
    fam_files = genotyped_subdir.glob('*.fam')
    most_recently_modified_fam_file = max([f.stat().st_mtime for f in fam_files])
    fam_cols = ['FID', 'IID', 'PID', 'MID', 'SEX', 'BATCH']
    fam = pl.read_csv(
        most_recently_modified_fam_file,
        has_header=False,
        new_columns=fam_cols
    )
    
    return fam