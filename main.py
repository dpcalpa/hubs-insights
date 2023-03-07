# imports
import doctest
import pandas as pd

POOR_RATIO = 10
CRITICAL_RATIO = 40
RATIO_CONSTANT = 2


class UnreachableHoleAnalysis:
    def __init__(self):
        self.abc = 0
        self.file_path = ''
        self.file_name = '2023 DE_case_dataset.gz.parquet'
        self.partitions = ['', '']
        pd.set_option('display.max_rows', 500)
        pd.set_option('display.max_columns', 500)
        pd.set_option('display.width', 0)

    def execute(self):
        """Wrapper function which calls the transformation functions"""
        self.unreachable_hole_analysis()

    def unreachable_hole_analysis(self):
        """Transforms data to get insights over unreachable holes warnings & errors"""
        # 1. Read from parquet to pandas dataframe
        raw_df = self.read_parquet()

        # 2. Create new columns

        # 3. Insights

        # 4. Write to parquet with partitions

        # print(res.columns)

    def read_parquet(self):
        """Reads from parquet file format
        Args
            arg1 (int): first argument

        Returns:
            Sum of num1 and num2
        #>>> uha.read_parquet() is not None
        """
        path = self.file_path
        file_name = self.file_name
        df = pd.read_parquet(f'{path}{file_name}', engine='fastparquet')
        return df

    def write_parquet(self):
        """"""
        path = self.file_path


if __name__ == '__main__':
    uha = UnreachableHoleAnalysis()
    uha.execute()

    doctest.testmod(extraglobs={'uha': UnreachableHoleAnalysis()})
