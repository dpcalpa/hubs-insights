# imports
import doctest
import json

import pandas as pd

POOR_RATIO = 10
CRITICAL_RATIO = 40
RATIO_CONSTANT = 2


class UnreachableHoleAnalysis:
    def __init__(self):
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

        # 2. Data exploration: one record
        # Printing holes data sample
        one_row_df = raw_df.query("holes == holes").iloc[0]
        one_row_dict = one_row_df.to_dict()
        for k, v in one_row_dict.items():
            print("- ", k, type(v))
            vn = json.loads(v) if '{' in str(v) else v
            print(f"    {vn}")

        holes_str = one_row_dict['holes']
        holes_dict = json.loads(holes_str)
        for idx, i in enumerate(holes_dict):
            print(f'Item {idx + 1} of {len(holes_dict)}')
            for k, v in i.items():
                print(f'    {k}: {v}')

        # Creating columns for data sample
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

        # 3. Create new columns
        has_unreachable_hole_warning_lst = raw_df['holes'].apply(
            lambda x: any([hole['length'] > hole['radius'] * RATIO_CONSTANT * POOR_RATIO if hole
                           else False
                           for hole in json.loads(str(x or '{}'))])
            ).to_list()
        has_unreacheable_hole_error_lst = raw_df['holes'].apply(
            lambda x: any([hole['length'] > hole['radius'] * RATIO_CONSTANT * CRITICAL_RATIO if hole
                           else False
                           for hole in json.loads(str(x or '{}'))])
            ).to_list()
        raw_df["has_unreachable_hole_warning"] = has_unreachable_hole_warning_lst
        raw_df["has_unreacheable_hole_error"] = has_unreacheable_hole_error_lst
        print(raw_df[['has_unreachable_hole_warning', 'has_unreacheable_hole_error']].iloc[:3])

        # 4. Some insights
        print('Count full df: ', len(raw_df.index))
        print('Count parts with holes: ', len(raw_df.query("holes == holes").index))
        print('Count parts with hole warnings: ', len(raw_df.query("has_unreachable_hole_warning == True").index))
        print('Count parts with hole errors: ', len(raw_df.query("has_unreacheable_hole_error == True").index))


        # 5. Write to parquet with partitions

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
