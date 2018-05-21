# -*- coding:utf-8 -*-
"""
@author:code37
@file:finance.py
@time:2018/4/279:46
"""
import pandas as pd
from datetime import datetime

def ttmContinues(report_df, label):
    """
    Compute Trailing Twelve Months for multiple indicator.
 
    computation rules:
        #. ttm indicator is computed on announcement date.
        #. on given release_date, use the latest report_date and the previous report year for computation. 
        #. if any report period is missing, use weighted method.
        #. if two reports (usually first-quoter and annual) are released together, only keep latest

    :param report_df: must have 'report_date', 'release_date', and <label> columns
    :type report_df: Pandas.DataFrame
    :param label: column name for intended indicator
    :type label: str.

    :return:  columned by ['datetime', 'report_date', <label>+'_TTM', ...]
    :rtype: Pandas.DataFrame

    .. todo::
       if announce_date exist, use announce_date instead of release_date, report_date as well
    """

    report_df = report_df.sort_values(by=['release_date', 'report_date'])
    report_df = report_df.drop_duplicates(subset=['release_date', 'report_date'])  # 剔除重复的数据
    report_df = report_df.reset_index(drop=True)

    month_ends = {3: 31, 6: 30, 9: 30, 12: 31}
    weights = {3 :4, 6 :2, 9: float(4)/3, 12: 1}

    label_list = label.replace(' ', '').split(',')
    ret = []
    for i, row in report_df.iterrows():
        if i < 4:    # 少于4份财报的不计算
            continue

        # get five report periods
        released_reports = report_df.ix[report_df.release_date<=row['release_date'], :]
        latest_report_year = released_reports.report_date.max().year
        latest_report_season = released_reports.report_date.max().month
        five_reports = list()
        year = latest_report_year
        season = latest_report_season
        for _ in range(5):
            value_for_report_period = dict()
            value_for_report_period['weight'] = weights[season]
            value_for_report_period['report_date'] = datetime(year=year, month=season, day=month_ends[season])

            season = season - 3
            if season <= 0:
                year = year - 1
                season = 12

            for label in label_list:
                report_value_for_label = released_reports.ix[released_reports.report_date == value_for_report_period['report_date'], label]
                # fill nan into missing period
                if len(report_value_for_label) == 0:
                    value = None
                else:
                    value = report_value_for_label.values[-1]
                # assign weights
                value_for_report_period[label] = value

            five_reports.append(value_for_report_period)

        # calculate TTM value
        five_reports = pd.DataFrame(five_reports).sort_values(by='report_date')
        five_reports.index = range(len(five_reports))
        #print five_reports

        ttm_value = dict()
        for label in label_list:
            if five_reports.ix[:, label].isnull().any(): # weighted method
                ttm_value[label + '_TTM'] = None
            else: # normal method
                normal_reports = five_reports.copy()
                normal_reports[label + '_SINGLE'] = normal_reports.ix[:, label].diff(1)
                normal_reports['report_month'] = normal_reports.report_date.apply(lambda x: x.month)

                if len(normal_reports[normal_reports.report_month == 3]) > 1:
                    normal_reports = normal_reports.iloc[1:]
                normal_reports.ix[normal_reports.report_month == 3, label + '_SINGLE'] = normal_reports.ix[normal_reports.report_month == 3, label]
                ttm_value[label + '_TTM'] = normal_reports.ix[:, label + '_SINGLE'].sum()

        ttm_value['datetime'] = row['release_date']
        ttm_value['report_date'] = row['report_date']

        ret.append(ttm_value)

    if len(ret) > 0:
        ret = pd.DataFrame(ret).drop_duplicates(subset=['datetime'], keep='last')
        ret = ret.dropna()
    return ret


def ttmDiscrete(report_df, label_str, min_report_num=4):
    """
    
    :param report_df: must have 'report_date', 'release_date', and <label> columns
    :type report_df: Pandas.DataFrame
    :param label_str: 
    :param min_report_num:
    :type min_report_num: int
    
    :return:  columned by ['datetime', 'report_date', <label>+'_TTM', ...]
    :rtype: pd.DataFrame

    """
    label_list = label_str.replace(' ', '').split(',')

    report_df = report_df.sort_values(by=['report_date', 'release_date'], ascending=[False, False])
    report_df = report_df.drop_duplicates(subset=['release_date', 'report_date'])  # 剔除重复的数据

    release_dates = list(set(report_df['release_date']))
    release_dates.sort(reverse=True)
    report_dates = report_df.groupby('release_date')['report_date'].first().values[::-1]
    result_dict = []

    for release_dt in release_dates:
        label_dict = {}
        for label in label_list:
            label_dict[label + '_TTM'] = report_df[report_df['release_date']<=release_dt].drop_duplicates(subset=['report_date']).head(min_report_num)[label].mean()
        result_dict.append(label_dict)

    ret_df = pd.DataFrame(result_dict)
    ret_df['datetime'] = release_dates
    ret_df['report_date'] = report_dates

    ret_df = ret_df.sort_values(by=['report_date', 'datetime'])
    return ret_df
