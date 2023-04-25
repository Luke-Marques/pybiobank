import unittest
import polars as pl
from polars.testing import assert_frame_equal
from pathlib import Path
from pybiobank.genotype import read_fam_file


class DFTests(unittest.TestCase):
    """Class for running unit tests on data parsing functions."""
    
    def test_read_fam_file(self):
        """
        Tests the function read_fam_file in the genotype module by comparing the output
        to a file generated using ukbkings [https://github.com/kenhanscombe/ukbkings] from the
        UK Biobank project 23203 on the King's HPC, CREATE.
        """
        TEST_DATA_DIR = Path('data')
        test_fam_file = TEST_DATA_DIR / 'pybiobank.test.ukb23203.fam'
        ukb_project_dir = Path(
            '/scratch',
            'prj',
            'premandm',
            'ukb23203_rga'
        )
        test_fam = pl.read_csv(test_fam_file, separator='\t')
        fam = read_fam_file(ukb_project_dir=ukb_project_dir)
        assert_frame_equal(test_fam, fam)
        
    def test_read_sample_quality_control_file(self):
        """
        Tests the function read_sample_quality_control_file in the genotype module by comparing 
        the output to a file generated using ukbkings [https://github.com/kenhanscombe/ukbkings] 
        from the UK Biobank project 23203 on the King's HPC, CREATE.
        """
        TEST_DATA_DIR = Path('data')
        test_sqc_file = TEST_DATA_DIR / 'pybiobank.test.ukb23203.sqc'
        ukb_project_dir = Path(
            '/scratch',
            'prj',
            'premandm',
            'ukb23203_rga'
        )
        test_sqc = pl.read_csv(test_sqc_file, separator='\t')
        sqc = read_fam_file(ukb_project_dir=ukb_project_dir)
        assert_frame_equal(test_sqc, sqc)