import pandas as pd
import streamlit as st
import requests

from data_collector import (
    create_dart,
    get_company_info,
    fetch_company_status,
    get_financial_statements,
    get_quarterly_financial_statements,
    get_reports,
    collect_employee_data,
    collect_dividend_data,
    collect_stock_data,
    collect_price_data
)

from report_generator import (
    load_report_data,
    parse_company_info,
    fetch_additional_company_info,
    parse_annual_income_statements,
    parse_quarterly_income_statements,
    parse_annual_balance_sheets,
    parse_annual_cash_flow,
    parse_dividend,
    parse_labor_salary,
    parse_stocks,
    parse_research_and_development,
    parse_report_urls,
    save_to_excel,
)


# pandas 설정
pd.set_option('display.max_colwidth', None)
pd.options.display.float_format = '{:.0f}'.format

st.title("📊 기업 정보 및 재무재표")

# 1️⃣ API Key 입력 및 유효성 검사
input_key = st.text_input(
    "🔑 OpenDART API 키",
    value=st.session_state.get("my_api", ""),
    disabled=st.session_state.get("api_verified", False)
)
api_key = input_key.strip().strip("'").strip('"')

def check_api_key_validity(api_key: str) -> bool:
    url = f"https://opendart.fss.or.kr/api/company.json?crtfc_key={api_key}&corp_code=00126380"
    try:
        res = requests.get(url, timeout=5)
        if "status" in res.text:
            data = res.json()
            return data["status"] == "000"
        return False
    except Exception as e:
        print("API 요청 실패:", e)
        return False

if st.button("✅ API 키 확인"):
    if check_api_key_validity(api_key):
        st.session_state.api_verified = True
        st.session_state.my_api = api_key
        st.session_state.api_success_message = "✅ 유효한 API 키입니다!"
    else:
        st.error("❌ 유효하지 않은 API 키입니다.")
        st.session_state.api_verified = False
        st.session_state.api_success_message = None

if st.session_state.get("api_success_message"):
    st.success(st.session_state.api_success_message)

# 2️⃣ 종목 코드 입력
if st.session_state.get("api_verified", False):
    input_code = st.text_input("📦 종목 코드 입력")
    s_code = input_code.strip().strip("'").strip('"')

    if st.button("💾 종목 코드 확인"):
        with st.spinner("🔎 종목코드 확인 중입니다..."):
            dart = create_dart(api_key)
            s_dict = dart.company(s_code)

            if s_dict["status"] == "000":
                s_name = s_dict['stock_name']
                st.session_state.s_name = s_name
                st.session_state.s_code_verified = True
                st.session_state.s_code = s_code
                st.session_state.s_code_success_message = f"✅ {s_name}"

                st.session_state.s_year = 2015
                st.session_state.e_year = 2025
                st.session_state.s_date = "2016-01-01"
                st.session_state.quarter_code = {
                    '1분기': '11013',
                    '2분기': '11012',
                    '3분기': '11014',
                    '사업보고서': '11011'
                }
            else:
                st.error("❌ 유효하지 않은 종목 코드입니다.")
                st.session_state.s_code_verified = False
                st.session_state.s_code_success_message = None

if st.session_state.get("s_code_success_message"):
    st.success(st.session_state.s_code_success_message)

# 3️⃣ 실행 버튼
if st.session_state.get("s_code_verified", False):
    if st.button(f"🚀 {st.session_state.s_name} 데이터 수집 실행"):
        with st.spinner("📂 데이터를 수집 중입니다... 15초 가량 소요"):
            dart = create_dart(st.session_state.my_api)
            report_data = {}

            dic = get_company_info(dart, st.session_state.s_code)
            s_name = dic['corp_name']
            report_data["s_name"] = s_name
            report_data["company_dict"] = dic

            status_df, m_holder_df = fetch_company_status(st.session_state.s_code)
            report_data["status_df"] = status_df
            report_data["m_holder_df"] = m_holder_df

            year_range = range(st.session_state.s_year, st.session_state.e_year + 1)
            n_year_range = range(2018, st.session_state.e_year + 1)

            report_data["fs_list"] = get_financial_statements(dart, st.session_state.s_code, year_range)
            report_data["qfs_list"] = get_quarterly_financial_statements(dart, st.session_state.s_code, n_year_range, st.session_state.quarter_code)
            report_data["e_list"] = collect_employee_data(dart, st.session_state.s_code, year_range)
            report_data["d_list"] = collect_dividend_data(dart, st.session_state.s_code, year_range)
            report_data["u_df"] = get_reports(dart, st.session_state.s_code, st.session_state.s_date)
            report_data["p_df"] = collect_price_data(st.session_state.s_code, st.session_state.s_year, st.session_state.e_year)
            report_data["s_list"] = collect_stock_data(dart, st.session_state.s_code, year_range)

            st.session_state["report_data"] = report_data
            st.success("✅ 데이터 수집 완료!")


# 4️⃣ 데이터 미리보기
if "report_data" in st.session_state:

    # report_data 가져오기
    report_data = st.session_state["report_data"]
    
    # 필요한 변수 모두 불러오기
    fs_list = report_data["fs_list"]
    qfs_list = report_data["qfs_list"]
    d_list = report_data["d_list"]
    e_list = report_data["e_list"]
    s_list = report_data["s_list"]
    u_df = report_data["u_df"]
    p_df = report_data["p_df"]
    company_dict = report_data["company_dict"]
    s_name = report_data["s_name"]
    status = report_data["status_df"]
    m_holder = report_data["m_holder_df"]


    if st.button("기업 정보 가져오기"):

        company_df = parse_company_info(company_dict)
        cov, product, capital, related, affiliate = fetch_additional_company_info(s_code)

        st.write("기업 정보 :")
        st.dataframe(company_df)
        st.dataframe(status)
        st.dataframe(cov)
        st.dataframe(m_holder)

    if st.button("연간 손익계산서 가져오기"):

        i_s, add_list = parse_annual_income_statements(fs_list, industry=None)
        st.write("연간 손익계산서 :")
        st.dataframe(i_s)

        if add_list :
            st.write("주의할 값 :")
            for df in add_list :
                st.dateframe(df)