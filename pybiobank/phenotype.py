import polars as pl
from pathlib import Path, PosixPath, WindowsPath
from typing import List
from functools import reduce


def read_ukb_field_finder(
    ukb_project_dir: str | Path | PosixPath | WindowsPath,
    ukb_project_phenotype_subdir_name: str | Path | PosixPath | WindowsPath = Path('phenotypes')
) -> pl.DataFrame:
    """Reads the UKB field finder files for each UKB project-specific basket available in the phenotypes sub-directory and then combines and returns a single Polars DataFrame.

    Args:
        ukb_project_dir (str or Path or PosixPath or WindowsPath): Path to UKB project top-level directory.
        ukb_project_phenotype_subdir_name (strorPathorPosixPathorWindowsPath, optional): Name of phenotype sub-directory within UKB project top-level directory. Defaults to 'phenotype'.

    Returns:
        pl.DataFrame: Polars DataFrame containing concatenated data from all field_finder.txt files within phenotypes sub-directory.
    """
    
    # ensure paths are Path objects
    ukb_project_dir = Path(ukb_project_dir)
    ukb_project_phenotype_subdir_name = Path(ukb_project_phenotype_subdir_name)
    
    # check if directories are valid, real paths
    if not ukb_project_dir.exists():
        raise FileNotFoundError(f'UK Biobank project directory ({ukb_project_dir}) does not exist.')
    elif not (ukb_project_dir / ukb_project_phenotype_subdir_name).exists():
        raise FileNotFoundError(f'UK Biobank project phenotype sub-directory ({ukb_project_dir / ukb_project_phenotype_subdir_name}) does not exist.')
    else:
        pass
    
    # get field finder files from phenotype sub-directory
    field_finder_filename_pattern = 'ukb*field_finder.txt'
    field_finder_files = sorted((ukb_project_dir / ukb_project_phenotype_subdir_name).glob(field_finder_filename_pattern))
    
    # read field finder files to Polars data frames
    field_finder_dfs = [
        pl.read_csv(f, separator='\t', has_header=True).with_columns([
            pl.lit(str(f.stem).split('_')[0]).alias('basket')
        ]) for f in field_finder_files
    ]
    field_finder = pl.concat(field_finder_dfs) \
        .unique() \
        .filter(pl.col('field') != 'eid')
    
    return field_finder


def read_ukb_phenotype_fields(
    ukb_project_dir: str | Path | PosixPath | WindowsPath,
    ukb_fields: List[str],
    ukb_project_phenotype_subdir_name: str | Path | PosixPath | WindowsPath = Path('phenotypes'),
    readable_column_headers: bool = False
) -> pl.DataFrame:
    """Reads the phenotype data, across all UKB project-specific baskets in the phenotypes sub-directory, for a subset of UKB data-fields and returns a Polars DataFrame of the data.
    Periodically, the UKB will update some subset of the data, e.g., hospital episode statistics. When this happens, the datafame created will include all duplicates with the basket ID as suffix 
    ("_<basket_id>"). Decide which to keep, larger basket numbers correspond to more recent data. To use \code{\link{bio_rename}}, to update the numeric field names to more descriptive names, first drop 
    duplicates you do not want and rename the remaining fields by deleting the "_<basket_id>" suffix.

    Args:
        ukb_project_dir (str | Path | PosixPath | WindowsPath): Path to UKB project top-level directory.
        ukb_fields (List[str]): List of UKB data-field codes in the format: field-instance.array.
        ukb_project_phenotype_subdir_name (str | Path | PosixPath | WindowsPath, optional): Name of phenotype sub-directory within UKB project top-level directory. Defaults to Path('phenotype').
        readable_column_headers (bool, optional): Transform column names in the resultant data frame to readable names instead of the UKB field codes. Defaults to False.

    Returns:
        pl.DataFrame: Polars DataFrame containing phenotype data for all fields indicated in the ukb_fields parameter.
    """
    
    # ensure paths are Path objects
    ukb_project_dir = Path(ukb_project_dir)
    ukb_project_phenotype_subdir_name = Path(ukb_project_phenotype_subdir_name)
    
    # check if directories are valid, real paths
    if not ukb_project_dir.exists():
        raise FileNotFoundError(f'UK Biobank project directory ({ukb_project_dir}) does not exist.')
    elif not (ukb_project_dir / ukb_project_phenotype_subdir_name).exists():
        raise FileNotFoundError(f'UK Biobank project phenotype sub-directory ({ukb_project_dir / ukb_project_phenotype_subdir_name}) does not exist.')
    else:
        pass
    
    # get field_finder data frame and filter to required fields
    field_finder = read_ukb_field_finder(
        ukb_project_dir=ukb_project_dir, 
        ukb_project_phenotype_subdir_name=ukb_project_phenotype_subdir_name
    ) \
        .filter(pl.col('field').is_in(ukb_fields))
    
    # get duplicated field columns
    dup_fields = field_finder.filter(pl.col('field').is_duplicated())['field'].unique().to_list()
    
    # create dictionary to map UKB dtypes to Polars dtypes
    dtype_dict = {
        'Sequence': pl.Int64,
        'Integer': pl.Int64,
        'Categorical (single)': pl.Utf8,
        'Categorical (multiple)': pl.Utf8,
        'Continuous': pl.Float64,
        'Text': pl.Utf8,
        'Date': pl.Date,
        'Time': pl.Time,
        'Compound': pl.Utf8,
        'Binary object': pl.Binary,
        'Records': pl.Utf8,
        'Curve': pl.Utf8
    }
    
    # loop over baskets in filtered field_finder and read required fields from phenotype .csv file
    baskets = field_finder['basket'].unique().to_list()
    pheno_dfs = []
    for basket in baskets:
        fields = ['eid'] + field_finder.filter(pl.col('basket') == basket)['field'].to_list()
        dtypes = [pl.Int64] + list(map(lambda x: dtype_dict.get(x, pl.Unknown), field_finder.filter(pl.col('basket') == basket)['ukb_type'].to_list()))
        pheno_file = ukb_project_dir / ukb_project_phenotype_subdir_name / Path(f'{basket}.csv')
        pheno_df = pl.read_csv(pheno_file, columns=fields, has_header=True, dtypes=dtypes)
        pheno_df.select(pl.all().map_alias(lambda col_name: f'{col_name}_{basket}' if (col_name in dup_fields) else col_name))  # add suffix to duplicated field columns
        pheno_dfs.append(pheno_df)
    pheno = reduce(lambda left_df, right_df: left_df.join(other=right_df, on='eid', how='outer'), pheno_dfs)

    return pheno
