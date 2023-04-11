import polars as pl
from pathlib import Path, PosixPath, WindowsPath
from typing import List
from functools import reduce

def read_ukb_field_finder(
    ukb_project_dir: str or Path or PosixPath or WindowsPath,
    ukb_project_phenotype_subdir_name: str or Path or PosixPath or WindowsPath = Path('phenotypes')
) -> pl.DataFrame:
    """_summary_

    Args:
        ukb_project_dir (strorPathorPosixPathorWindowsPath): _description_
        ukb_project_phenotype_subdir_name (strorPathorPosixPathorWindowsPath, optional): _description_. Defaults to Path('phenotype').

    Returns:
        pl.DataFrame: _description_
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
    
    # get basket names (e.g. ukbXXXXX) from field finder files
    baskets = (str(f.stem).split('_')[0] for f in field_finder_files)
    
    # read field finder files to Polars data frames
    field_finder_dfs = [pl.read_csv(f, sep='\t', has_header=True).with_columns([pl.lit(str(f.stem).split('_')[0]).alias('basket'), pl.lit(str(str(f.absolute()))).alias('path')]) for f in field_finder_files]
    field_finder = pl.concat(field_finder_dfs) \
        .unique() \
        .filter(pl.col('field') != 'eid')
    
    return field_finder


def read_ukb_phenotype_fields(
    ukb_project_dir: str or Path or PosixPath or WindowsPath,
    ukb_fields: List[str],
    ukb_project_phenotype_subdir_name: str or Path or PosixPath or WindowsPath = Path('phenotypes'),
    exact: bool = True
) -> pl.DataFrame:
    """_summary_

    Args:
        ukb_project_dir (strorPathorPosixPathorWindowsPath): _description_
        ukb_fields (List[str or int]): _description_
        ukb_project_phenotype_subdir_name (strorPathorPosixPathorWindowsPath, optional): _description_. Defaults to Path('phenotypes').

    Returns:
        pl.DataFrame: _description_
    """
    
    # ensure paths are Path objects
    ukb_project_dir = Path(ukb_project_dir)
    ukb_project_phenotype_subdir_name = Path(ukb_project_phenotype_subdir_name)
    
    # check if directories are valid, real paths
    if not ukb_project_dir.exists():
        raise FileNotFoundError(f'UK Biobank project directory ({ukb_project_dir}) does not exist.')
    elif not (ukb_project_dir / ukb_project_phenotype_subdir_name).exists():
        raise FileNotFoundError(f'UK Biobank project phenotype sub-directory ({ukb_project_phenotype_subdir_name}) does not exist.')
    else:
        pass
    
    # get field_finder data frame and filter to required fields
    if exact:
        field_finder = read_ukb_field_finder(
            ukb_project_dir=ukb_project_dir, 
            ukb_project_phenotype_subdir_name=ukb_project_phenotype_subdir_name
        ) \
            .filter(pl.col('field').is_in(ukb_fields))
    else:
        pass  # TO-DO: add in messy filtering of ukb_fields
    
    # loop over baskets in filtered field_finder and read required fields from phenotype .csv file
    baskets = field_finder['basket'].unique().to_list()
    pheno_dfs = []
    for basket in baskets:
        fields = ['eid'] + field_finder.filter(pl.col('basket') == basket)['field'].unique().to_list()
        pheno_file = ukb_project_dir / ukb_project_phenotype_subdir_name / Path(f'{basket}.csv')
        pheno_dfs.append(pl.read_csv(pheno_file, columns=fields, has_header=True, infer_schema_length=1000))
    pheno = reduce(lambda left_df, right_df: left_df.join(other=right_df, on='eid', how='outer'), pheno_dfs)
    
    return pheno
