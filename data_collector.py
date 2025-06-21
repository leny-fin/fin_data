"""
데이터 수집 함수 모음
ex) Dart API / 주가 / 보고서 등 
"""
# data_collector.py
import OpenDartReader
import pandas as pd
import FinanceDataReader as fdr
import requests
from io import StringIO

def create_dart(api_key):
    dart = OpenDartReader(api_key)
    return dart

def get_company_info(dart, code):
    return dart.company(code)

# company_status 저장하는 함수
def fetch_company_status(code):
    
    # url 작성
    url_tmpl_1 = 'http://companyinfo.stock.naver.com/v1/company/c1010001.aspx?cmp_cd=%s'
    url_1 = url_tmpl_1 % code

    # url의 table을 dfs에 저장 (리스트)
    html_text_1 = requests.get(url_1).text
    dfs = pd.read_html(StringIO(html_text_1))

    # 기준일 저장
    baseline_date = dfs[3].loc[0,0]

    # status_df 저장
    dfs[1].rename(columns={0:'항목', 1:baseline_date}, inplace=True)

    # 주가/전일대비/수익률에서 주가만 남기기
    price = dfs[1].iloc[0, 1].split('/')[0].strip()

    dfs[1].iloc[0, 0] = '주가'
    dfs[1].iloc[0, 1] = price

    status = dfs[1].iloc[[0,4,2,7]]
    status_df = status.reset_index(drop=True)

    # m_holer_df 저장
    m_holder_df = dfs[4]

    return status_df, m_holder_df

def get_financial_statements(dart, code, year_range):
    fs_list = []
    for year in year_range:
        try:
            df = dart.finstate_all(code, year)[['sj_div','account_id','account_nm','thstrm_amount']]
            df.columns.name = year
            df.index.name = "연결재무제표"
            fs_list.append(df)
        except KeyError:
            try:
                df = dart.finstate_all(code, year, fs_div='OFS')[['sj_div','account_id','account_nm','thstrm_amount']]
                df.columns.name = year
                df.index.name = "별도재무제표"
                fs_list.append(df)
            except:
                print(f"{year} 오류")
    return fs_list

def get_quarterly_financial_statements(dart, code, year_range, quarter_code):
    qfs_list = []
    for year in year_range:
        q_code = {'11013': '1분기','11012' : '2분기','11014':'3분기'}
        for rep_code, q_name in q_code.items():
            try:
                df = dart.finstate_all(code, year, reprt_code=rep_code)[['sj_div','account_id','account_nm','thstrm_amount']]
                df.columns.name = f"{year} {q_name}"
                df.index.name = "연결재무제표"
                qfs_list.append(df)
            except KeyError:
                try:
                    df = dart.finstate_all(code, year, reprt_code=rep_code, fs_div='OFS')[['sj_div','account_id','account_nm','thstrm_amount']]
                    df.columns.name = f"{year} {q_name}"
                    df.index.name = "별도재무제표"
                    qfs_list.append(df)
                except:
                    print(f"{year} 오류")
    return qfs_list

def get_reports(dart, code, start_date):
    df = dart.list(code, start=start_date, kind='A')
    report_df = df[df['report_nm'].str.contains('사업보고서')]
    report_df = report_df[["corp_name", "stock_code", "report_nm", "rcept_no"]]
    rcept_list = report_df['rcept_no']

    report_url_list = [f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={no}" for no in rcept_list]
    report_df['report_url'] = report_url_list

    report_df[['report_nm', 'year']] = report_df['report_nm'].str.split('(', expand=True)
    report_df['year'] = report_df['year'].str.split('.', expand=True)[0]
    report_df = report_df[['year'] + [col for col in report_df.columns if col != 'year']]

    des_url_list = []
    for rcp, year in zip(report_df['rcept_no'], report_df['year']):
        print(f"{year} 사업의 내용 URL")
        des_url_list.append(dart.sub_docs(rcp, match="사업의 내용")["url"].values[0])
    report_df["des_url_list"] = des_url_list

    return report_df

def collect_employee_data(dart, code, year_range):
    e_list = []
    for year in year_range:
        df = dart.report(code, '직원', year)
        if df.empty:
            print(f"{year} 자료 없음")
        else:
            df.columns.name = year
            e_list.append(df)
    return e_list

def collect_dividend_data(dart, code, year_range):
    d_list = []
    for year in year_range:
        df = dart.report(code, '배당', year)
        if df.empty:
            print(f"{year} 자료 없음")
        else:
            df.columns.name = year
            d_list.append(df)
    return d_list

def collect_stock_data(dart, code, year_range):
    s_list = []
    for year in year_range:
        df = dart.report(code, '주식총수', year)
        if df.empty:
            print(f"{year} 자료 없음")
        else:
            df.columns.name = year
            s_list.append(df)
    return s_list

def collect_price_data(code, start_year, end_year):
    p_df = pd.DataFrame(columns=['평균가격','고가','저가'])
    df = fdr.DataReader(code, f'{start_year}')[['High','Low','Close']]
    df['연도'] = df.index.year
    group = df.groupby('연도')
    for year in range(start_year, end_year+1):
        try:
            p_df.loc[year, '평균가격'] = int(group.get_group(year)['Close'].mean())
            p_df.loc[year, '고가'] = group.get_group(year)['High'].max()
            p_df.loc[year, '저가'] = group.get_group(year)['Low'].min()
        except:
            print(f'{year}년 자료가 없습니다.')
    return p_df