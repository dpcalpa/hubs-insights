# imports
import doctest
import json

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
        print('Count: ', len(raw_df.index))

        # todo remove filtering
        raw_df = raw_df.query("holes == holes")
        print('Count: ', len(raw_df.index))
        # -------------------
        # Print one record todo remove
        one_row_df = raw_df.iloc[0]
        one_row_dict = one_row_df.to_dict()
        print(one_row_dict)
        print(one_row_dict['holes'])
        holes_str = one_row_dict['holes']
        holes_dict = json.loads(holes_str)
        print(holes_dict)
        for idx, i in enumerate(holes_dict):
            print(f'Item {idx+1} of {len(holes_dict)}')
            for k, v in i.items():
                print(f'    {k}: {v}')
        # has_unreachable_hole_warning
        bool_lst = [True
                    if h['length'] > h['radius'] * RATIO_CONSTANT * POOR_RATIO
                    else False
                    for h in holes_dict]
        print(bool_lst)
        has_unreachable_hole_warning = any(bool_lst)
        print('has_unreachable_hole_warning', has_unreachable_hole_warning)
        # has_unreacheable_hole_error
        bool_lst = [True
                    if h['length'] > h['radius'] * RATIO_CONSTANT * CRITICAL_RATIO
                    else False
                    for h in holes_dict]
        print(bool_lst)
        has_unreacheable_hole_error = any(bool_lst)
        print('has_unreacheable_hole_error', has_unreacheable_hole_error)
        # -------------------
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
