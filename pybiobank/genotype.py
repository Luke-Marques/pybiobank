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
    fam_cols = ['fid', 'iid', 'pid', 'mid', 'sex', 'batch']
    fam = pl.read_csv(
        most_recently_modified_fam_file,
        has_header=False,
        new_columns=fam_cols
    )
    
    return fam


def read_sample_quality_control_file(ukb_project_dir: Path) -> pl.DataFrame:
    """_summary_

    Args:
        ukb_project_dir (Path): _description_

    Returns:
        pl.DataFrame: _description_
    """
    
    imputed_subdir = ukb_project_dir / 'imputed'
    
    # read in sample quality control file
    sample_qc_cols = [col.lower() for col in chain(*[
        "affymetrix_1", 
        "affymetrix_2", 
        "genotyping_array", 
        "Batch",
        "Plate_Name", 
        "Well", 
        "Cluster_CR", 
        "dQC", 
        "Internal_Pico_ng_uL",
        "Submitted_Gender", 
        "Inferred_Gender", 
        "X_intensity", 
        "Y_intensity",
        "Submitted_Plate_Name", 
        "Submitted_Well", 
        "sample_qc_missing_rate",
        "heterozygosity", 
        "heterozygosity_pc_corrected",
        "het_missing_outliers", 
        "putative_sex_chromosome_aneuploidy",
        "in_kinship_table", 
        "excluded_from_kinship_inference",
        "excess_relatives", 
        "in_white_British_ancestry_subset",
        "used_in_pca_calculation",
        [f'pc{i+1}' for i in range(40)],
        "in_Phasing_Input_chr1_22", 
        "in_Phasing_Input_chrX",
        "in_Phasing_Input_chrXY"
    ])]
    sample_qc_file = imputed_subdir / 'ukb_sqc_v2.txt'
    sample_qc = pl.read_csv(
        sample_qc_file,
        has_header=False,
        new_columns=sample_qc_cols
    )
    
    # read fid values from fam file to list
    fam_fids = read_fam_file(ukb_project_dir=ukb_project_dir)['fid'].to_list()
    
    # check if sample quality control and fam files are same length
    if (nrows_sample_qc := len(sample_qc)) != (nrows_fam := len(fam_fids)):
        error_msg = f'Number of samples (rows) in the sample quality control file ({nrows_sample_qc} samples) do not match fam file ({nrows_fam} samples).'
        raise AssertionError(error_msg)
    
    # add eid column (fid values from fam file) to sample quality control dataframe
    sample_qc['eid'] = fam_fids
    sample_qc_reordered_cols = ['eid'] + [col for col in sample_qc.columns.difference('eid')]
    sample_qc = sample_qc[sample_qc_reordered_cols]
    
    return sample_qc
    

def read_relatedness_file(ukb_project_dir: Path) -> pl.DataFrame:
    """_summary_

    Args:
        ukb_project_dir (Path): _description_

    Returns:
        pl.DataFrame: _description_
    """
    
    imputed_subdir = ukb_project_dir / 'imputed'
    
    # read relatedness files to single dataframe
    relatedness_files = imputed_subdir.glob('*rel*.dat')
    rel = pl.concat((pl.read_csv(f)) for f in relatedness_files)
    
    return rel
    
    