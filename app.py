import re
import uuid
from datetime import datetime
import time
import random

import pandas as pd
import streamlit as st

import gspread
from google.oauth2.service_account import Credentials

# -----------------------------
# Config
# -----------------------------
st.set_page_config(page_title="가계부", layout="centered", initial_sidebar_state="collapsed")

INCOME_CATS = ["월급", "부수입", "이자", "캐시백", "기타"]
EXPENSE_CATS = ["식재료", "외식/배달", "생활", "육아", "여가", "교통/유류", "의료", "기타"]

TAB_NAMES = ["가계부", "예산", "고정지출", "경조사비", "제로페이", "신용카드"]

SHEET_NAMES = {
    "ledger": "ledger",
    "budgets": "budgets",
    "fixed_rules": "fixed_rules",
    "fixed_applied": "fixed_applied",
    "events": "events",
    "zeropay": "zeropay",
    "cards": "cards",
    "subscriptions": "subscriptions",
}

# -----------------------------
# UI (iPhone-friendly + Sticky Tabs)
# -----------------------------
st.markdown(
    """
<style>
.block-container {padding-top: 1.0rem; padding-bottom: 3.0rem; max-width: 860px;}
button, input, textarea {font-size: 16px !important;} /* iOS zoom 방지 */
div[data-testid="stTabs"] {position: sticky; top: 0; z-index: 999; background: white; padding-top: 0.2rem;}
div[data-testid="stTabs"] button {padding: 10px 12px;}
</style>
""",
    unsafe_allow_html=True,
)

# -----------------------------
# Helpers
# -----------------------------
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def ym_from_year_month(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"

def parse_yyyy_mm_dd(s: str):
    if not re.fullmatch(r"\d{4}/\d{2}/\d{2}", s.strip()):
        return None
    y, m, d = map(int, s.split("/"))
    try:
        datetime(y, m, d)
        return y, m, d
    except ValueError:
        return None

def day_to_korean(day: int) -> str:
    return f"{day:02d}일"

def to_int_amount(s: str):
    s = s.strip().replace(",", "")
    if not re.fullmatch(r"-?\d+", s):
        return None
    try:
        return int(s)
    except:
        return None

def fmt_amount(n: int) -> str:
    return f"{int(n):,}"

def style_money(df: pd.DataFrame, cols: list[str]):
    sty = df.style
    for c in cols:
        if c in df.columns:
            sty = sty.format({c: lambda x: fmt_amount(int(x)) if pd.notna(x) and str(x) != "" else ""})
            sty = sty.applymap(lambda v: "color:#d32f2f;" if isinstance(v, (int, float)) and v < 0 else "", subset=[c])
            sty = sty.set_properties(subset=[c], **{"text-align": "right"})
    return sty

def ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df

def cat_order_key(cat: str) -> int:
    try:
        return EXPENSE_CATS.index(cat)
    except ValueError:
        return 999

def month_filter(df: pd.DataFrame, ym: str) -> pd.DataFrame:
    if df.empty or "ym" not in df.columns:
        return df.iloc[0:0]
    return df[df["ym"].astype(str) == ym].copy()

def col_to_a1(col_idx_1based: int) -> str:
    # 1->A, 2->B ...
    n = col_idx_1based
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def with_retry(fn, *, tries: int = 5, base_sleep: float = 0.6):
    """Google Sheets API는 429/5xx가 간헐적으로 발생할 수 있어 재시도로 안정성 확보."""
    last_err = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last_err = e
            sleep = base_sleep * (2 ** i) + random.uniform(0, 0.25)
            time.sleep(sleep)
    raise last_err



def run_once(key: str) -> bool:
    """버튼/폼을 연타해도 같은 rerun에서 중복 실행을 줄이기 위한 간단한 락."""
    if st.session_state.get(key):
        return False
    st.session_state[key] = True
    return True

# -----------------------------
# Google Sheets
# -----------------------------
@st.cache_resource
def gs_client():
    if "gcp_service_account" not in st.secrets or "GSHEET_URL" not in st.secrets:
        st.error("secrets에 GSHEET_URL / gcp_service_account 가 없습니다.")
        st.stop()
    creds = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)

@st.cache_resource
def gs_book():
    return gs_client().open_by_url(st.secrets["GSHEET_URL"])

def ws(key: str):
    return gs_book().worksheet(SHEET_NAMES[key])

@st.cache_data(ttl=600)
def read_df(key: str) -> pd.DataFrame:
    w = ws(key)
    rows = with_retry(lambda: w.get_all_records())
    return pd.DataFrame(rows)

def get_headers(key: str) -> list[str]:
    return with_retry(lambda: ws(key).row_values(1))

def safe_append_rows(key: str, rows: list[dict], dedup_key_field: str | None = None):
    """
    - USER_ENTERED append
    - dedup_key_field가 있으면 해당 컬럼만 읽어서 중복 방지(더블클릭 방지)
    """
    if not rows:
        return 0, 0

    w = ws(key)
    headers = with_retry(lambda: w.row_values(1))
    if not headers:
        raise RuntimeError(f"{key} 시트 1행 헤더가 비어있습니다.")

    existing = set()
    if dedup_key_field and (dedup_key_field in headers):
        col_idx = headers.index(dedup_key_field) + 1
        a1 = col_to_a1(col_idx)
        # 해당 컬럼만 읽기(최소 읽기)
        col_vals = with_retry(lambda: w.get(f"{a1}2:{a1}"))
        existing = set([r[0] for r in col_vals if r and r[0]])

    to_write = []
    skipped = 0
    for r in rows:
        if dedup_key_field and (dedup_key_field in headers):
            k = str(r.get(dedup_key_field, "")).strip()
            if k and k in existing:
                skipped += 1
                continue
            if k:
                existing.add(k)
        to_write.append([r.get(h, "") for h in headers])

    if not to_write:
        return 0, skipped

    with_retry(lambda: w.append_rows(to_write, value_input_option="USER_ENTERED"))
    return len(to_write), skipped

def find_row_by_id(key: str, row_id: str):
    """
    id 컬럼을 찾아서 row index(1-based) 리턴. 없으면 None.
    최소 읽기: id 컬럼만 가져와서 탐색.
    """
    headers = get_headers(key)
    if "id" not in headers:
        return None
    idx = headers.index("id") + 1
    a1 = col_to_a1(idx)
    w = ws(key)
    vals = with_retry(lambda: w.get(f"{a1}2:{a1}"))
    ids = [r[0] for r in vals if r]
    try:
        pos0 = ids.index(row_id)  # 0-based within data (excluding header)
        return 2 + pos0  # actual sheet row
    except ValueError:
        return None

def update_row_by_id(key: str, row_id: str, updates: dict):
    """
    updates: {header: value}
    해당 id의 행에서 지정 컬럼만 업데이트.
    """
    w = ws(key)
    headers = get_headers(key)
    r = find_row_by_id(key, row_id)
    if r is None:
        return False, "해당 ID 행을 찾지 못했습니다."

    # 한 번에 range 업데이트(최소 API)
    cells = []
    for k, v in updates.items():
        if k not in headers:
            continue
        c = headers.index(k) + 1
        cells.append((r, c, v))

    if not cells:
        return False, "업데이트할 컬럼이 없습니다(헤더 확인)."

    # gspread는 batch_update_cells가 애매하니 범위별로 묶어서 한번에
    # 간단/안전 우선: update_cells 사용
    cell_objs = []
    for rr, cc, vv in cells:
        cell_objs.append(gspread.Cell(rr, cc, vv))
    with_retry(lambda: w.update_cells(cell_objs, value_input_option="USER_ENTERED"))
    return True, "수정 완료"

def delete_row_by_id(key: str, row_id: str):
    w = ws(key)
    r = find_row_by_id(key, row_id)
    if r is None:
        return False, "해당 ID 행을 찾지 못했습니다."
    with_retry(lambda: w.delete_rows(r))
    return True, "삭제 완료"

# -----------------------------
# App
# -----------------------------
st.title("📒 가계부 웹앱")

st.caption("데이터가 안 보이거나 최신이 아닐 때는 아래 버튼으로 새로고침하세요.")
if st.button("🔄 데이터 새로고침"):
    read_df.clear()
    st.success("새로고침 완료")


tabs = st.tabs(TAB_NAMES)
today = datetime.now()

# =============================
# 1) 가계부
# =============================
with tabs[0]:
    st.subheader("가계부")

    c1, c2 = st.columns(2)
    with c1:
        year = st.selectbox("연도", list(range(today.year - 3, today.year + 2)), index=3, key="ly")
    with c2:
        month = st.selectbox("월", list(range(1, 13)), index=today.month - 1, key="lm")
    ym = ym_from_year_month(year, month)

    # ---- 입력 ----
    st.markdown("#### 내역 입력")

    entry_type = st.selectbox("구분", ["수입", "지출"], key="ledger_type")
    cats = INCOME_CATS if entry_type == "수입" else EXPENSE_CATS

    # 구분 변경 시 카테고리 자동 초기화
    if st.session_state.get("ledger_last_type") != entry_type:
        st.session_state["ledger_category"] = cats[0]
        st.session_state["ledger_last_type"] = entry_type

    with st.form("ledger_add_form", clear_on_submit=True):
        d1, d2 = st.columns(2)
        with d1:
            date_str = st.text_input("날짜 (0000/00/00)", value=today.strftime("%Y/%m/%d"))
        with d2:
            category = st.selectbox("카테고리", cats, key="ledger_category")

        a1, a2 = st.columns([1, 2])
        with a1:
            amount_str = st.text_input("금액 (예: 12,345 / -12,345)", value="")
        with a2:
            memo = st.text_input("메모", value="")
        ok = st.form_submit_button("저장", type="primary")
    if ok and run_once("ledger_add"):
        parsed = parse_yyyy_mm_dd(date_str)
        amt = to_int_amount(amount_str)
        if not parsed:
            st.error("날짜 형식을 확인해 주세요. 예: 2026/03/03")
        elif amt is None:
            st.error("금액 형식을 확인해 주세요.")
        else:
            y, m, d = parsed
            row_ym = ym_from_year_month(y, m)
            row_day = day_to_korean(d)

            # 가능하면 ledger 자체도 dedup_key로 중복 방지(시트에 컬럼이 있을 때만)
            dk = f"LEDGER|{row_ym}|{row_day}|{entry_type}|{category}|{amt}|{memo.strip()}"
            row = {
                "id": str(uuid.uuid4()),
                "ym": row_ym,
                "day": row_day,
                "type": entry_type,
                "category": category,
                "amount": amt,
                "memo": memo.strip(),
                "created_at": now_str(),
                "dedup_key": dk,
            }
            wrote, skipped = safe_append_rows("ledger", [row], dedup_key_field="dedup_key")
            read_df.clear()
            if wrote:
                st.success("저장 완료")
            else:
                st.info("동일 내용이 이미 저장되어 있어 추가하지 않았습니다.")

    # ---- 데이터 1회 로드(탭 내 재사용) ----
    ledger = read_df("ledger")
    ledger = ensure_cols(ledger, ["id", "ym", "day", "type", "category", "amount", "memo", "created_at"])
    if not ledger.empty:
        ledger["amount"] = pd.to_numeric(ledger["amount"], errors="coerce").fillna(0).astype(int)

    # ---- 월 요약 ----
    this_month = month_filter(ledger, ym) if not ledger.empty else ledger.iloc[0:0]

    st.markdown("#### 월 요약")
    income_sum = int(this_month.loc[this_month["type"] == "수입", "amount"].sum()) if not this_month.empty else 0
    expense_sum = int(this_month.loc[this_month["type"] == "지출", "amount"].sum()) if not this_month.empty else 0
    balance_month = income_sum - expense_sum

    # 당해년도 누적(1월~선택월)
    y_prefix = f"{year:04d}-"
    ytd = ledger[ledger["ym"].astype(str).str.startswith(y_prefix)].copy() if not ledger.empty else ledger.iloc[0:0]
    if not ytd.empty:
        ytd = ytd[ytd["ym"].astype(str) <= ym].copy()
    ytd_income = int(ytd.loc[ytd["type"] == "수입", "amount"].sum()) if not ytd.empty else 0
    ytd_expense = int(ytd.loc[ytd["type"] == "지출", "amount"].sum()) if not ytd.empty else 0
    balance_ytd = ytd_income - ytd_expense

    bmode = st.radio("잔액 보기", ["선택 월 기준", "당해년도 월 누적"], horizontal=True)
    shown_balance = balance_month if bmode == "선택 월 기준" else balance_ytd

    m1, m2, m3 = st.columns(3)
    m1.metric("수입합계", fmt_amount(income_sum))
    m2.metric("지출합계", fmt_amount(expense_sum))
    m3.metric("잔액", fmt_amount(shown_balance))

    # ---- 예산현황 ----
    st.markdown("#### 예산현황")
    budgets = read_df("budgets")
    budgets = ensure_cols(budgets, ["id", "ym", "category", "target", "dedup_key"])
    budgets_m = budgets[budgets["ym"].astype(str) == ym].copy() if not budgets.empty else budgets.iloc[0:0]

    if budgets_m.empty:
        st.info("선택한 월의 예산이 없습니다. 예산 탭에서 설정해 주세요.")
    else:
        budgets_m["target"] = pd.to_numeric(budgets_m["target"], errors="coerce").fillna(0).astype(int)

        exp = this_month[this_month["type"] == "지출"].copy()
        exp_by_cat = exp.groupby("category")["amount"].sum().to_dict() if not exp.empty else {}

        view = pd.DataFrame({
            "카테고리": budgets_m["category"].astype(str),
            "목표금액": budgets_m["target"].astype(int),
        })
        view["실제지출금액"] = view["카테고리"].map(lambda c: int(exp_by_cat.get(c, 0)))
        view["차액"] = view["목표금액"] - view["실제지출금액"]

        # 카테고리 순서 유지(요구사항)
        view["__ord"] = view["카테고리"].map(cat_order_key)
        view = view.sort_values(["__ord", "카테고리"]).drop(columns="__ord")
        view = view[["카테고리", "목표금액", "실제지출금액", "차액"]]

        st.dataframe(
            view.style
            .format({
                "목표금액": lambda x: fmt_amount(int(x)),
                "실제지출금액": lambda x: fmt_amount(int(x)),
                "차액": lambda x: fmt_amount(int(x)),
            })
            .applymap(lambda v: "color:#d32f2f;" if isinstance(v, (int, float)) and v < 0 else "", subset=["차액"])
            .set_properties(subset=["목표금액", "실제지출금액", "차액"], **{"text-align": "right"}),
            use_container_width=True,
            hide_index=True,
        )

    # ---- 월 내역(지출) ----
    st.markdown("#### 지출내역")
    exp_rows = this_month[this_month["type"] == "지출"].copy()
    if exp_rows.empty:
        st.info("선택한 월의 지출 내역이 없습니다.")
    else:
        show = exp_rows[["day", "category", "amount", "memo", "created_at", "id"]].rename(
            columns={"day": "날짜", "category": "카테고리", "amount": "금액", "memo": "메모", "created_at": "등록시각", "id": "ID"}
        )
        st.dataframe(
            show.style
            .format({"금액": lambda x: fmt_amount(int(x))})
            .applymap(lambda v: "color:#d32f2f;" if isinstance(v, (int, float)) and v < 0 else "", subset=["금액"])
            .set_properties(subset=["금액"], **{"text-align": "right"}),
            use_container_width=True,
            hide_index=True,
        )

    # ---- 수정/삭제 ----
    st.markdown("#### 내역 수정/삭제")
    if this_month.empty:
        st.info("선택한 월에 수정/삭제할 내역이 없습니다.")
    else:
        # 사람이 보기 좋은 라벨 생성
        tmp = this_month.copy()
        tmp["label"] = tmp.apply(
            lambda r: f"{r.get('day','')} | {r.get('type','')} | {r.get('category','')} | {fmt_amount(int(r.get('amount',0)))} | {str(r.get('memo',''))[:12]}",
            axis=1,
        )
        options = dict(zip(tmp["label"].tolist(), tmp["id"].tolist()))
        sel_label = st.selectbox("대상 선택", list(options.keys()))
        sel_id = options[sel_label]

        sel_row = tmp[tmp["id"] == sel_id].iloc[0]

        with st.form("ledger_edit_form"):
            e1, e2 = st.columns(2)
            with e1:
                new_type = st.selectbox("구분", ["수입", "지출"], index=0 if sel_row["type"] == "수입" else 1)
            with e2:
                new_date = st.text_input("날짜 (0000/00/00)", value=f"{year:04d}/{month:02d}/{str(sel_row['day'])[:2]}")
            new_cats = INCOME_CATS if new_type == "수입" else EXPENSE_CATS
            new_cat = st.selectbox("카테고리", new_cats, index=max(0, (new_cats.index(sel_row["category"]) if sel_row["category"] in new_cats else 0)))
            new_amt = st.text_input("금액", value=fmt_amount(int(sel_row["amount"])))
            new_memo = st.text_input("메모", value=str(sel_row.get("memo", "")))

            cbtn1, cbtn2 = st.columns(2)
            with cbtn1:
                do_update = st.form_submit_button("수정 저장", type="primary")
            with cbtn2:
                do_delete = st.form_submit_button("삭제", type="secondary")
        if do_update and run_once("ledger_update"):
            parsed = parse_yyyy_mm_dd(new_date)
            amt = to_int_amount(new_amt)
            if not parsed:
                st.error("날짜 형식을 확인해 주세요. 예: 2026/03/03")
            elif amt is None:
                st.error("금액 형식을 확인해 주세요.")
            else:
                y, m, d = parsed
                row_ym = ym_from_year_month(y, m)
                row_day = day_to_korean(d)
                dk = f"LEDGER|{row_ym}|{row_day}|{new_type}|{new_cat}|{amt}|{new_memo.strip()}"

                ok2, msg = update_row_by_id(
                    "ledger",
                    sel_id,
                    {
                        "ym": row_ym,
                        "day": row_day,
                        "type": new_type,
                        "category": new_cat,
                        "amount": amt,
                        "memo": new_memo.strip(),
                        "dedup_key": dk,  # 컬럼 없으면 자동 무시됨
                    },
                )
                read_df.clear()
                st.success(msg) if ok2 else st.error(msg)
        if do_delete and run_once("ledger_delete"):
            ok2, msg = delete_row_by_id("ledger", sel_id)
            read_df.clear()
            st.success(msg) if ok2 else st.error(msg)

# =============================
# 2) 예산
# =============================
with tabs[1]:
    st.subheader("예산 설정")
    st.caption("저장을 두 번 눌러도 동일 월/카테고리/목표금액은 한 번만 반영됩니다.")

    c1, c2 = st.columns(2)
    with c1:
        by = st.selectbox("연도", list(range(today.year - 3, today.year + 2)), index=3, key="by")
    with c2:
        bm = st.selectbox("월", list(range(1, 13)), index=today.month - 1, key="bm")
    bym = ym_from_year_month(by, bm)

    with st.form("budget_add_form", clear_on_submit=True):
        cat = st.selectbox("지출 카테고리", EXPENSE_CATS)
        target_str = st.text_input("목표금액", placeholder="예: 300,000")
        ok = st.form_submit_button("저장하기", type="primary")
    if ok and run_once("budget_add"):
        t = to_int_amount(target_str)
        if t is None or t < 0:
            st.error("목표금액은 0 이상의 숫자로 입력해 주세요.")
        else:
            dk = f"{bym}|{cat}|{t}"
            row = {
                "id": str(uuid.uuid4()),
                "ym": bym,
                "category": cat,
                "target": t,
                "created_at": now_str(),
                "dedup_key": dk,
            }
            wrote, _ = safe_append_rows("budgets", [row], dedup_key_field="dedup_key")
            read_df.clear()
            st.success("저장 완료") if wrote else st.info("이미 동일 예산이 있어 추가하지 않았습니다.")

    st.markdown("#### 해당 월 예산 목록")
    budgets = read_df("budgets")
    budgets = ensure_cols(budgets, ["ym", "category", "target"])
    b = budgets[budgets["ym"].astype(str) == bym].copy() if not budgets.empty else budgets.iloc[0:0]
    if b.empty:
        st.info("해당 월 예산이 없습니다.")
    else:
        b["target"] = pd.to_numeric(b["target"], errors="coerce").fillna(0).astype(int)
        view = b[["category", "target"]].rename(columns={"category": "카테고리", "target": "목표금액"})
        view["__ord"] = view["카테고리"].map(cat_order_key)
        view = view.sort_values(["__ord", "카테고리"]).drop(columns="__ord")
        st.dataframe(
            view.style.format({"목표금액": lambda x: fmt_amount(int(x))})
            .set_properties(subset=["목표금액"], **{"text-align": "right"}),
            use_container_width=True,
            hide_index=True,
        )

# =============================
# 3) 고정지출
# =============================
with tabs[2]:
    st.subheader("고정지출")
    st.caption("규칙 등록 → 월 선택 반영 (중복 방지: 동일 월/항목/금액 1회)")

    st.markdown("#### 1) 고정지출 규칙 등록")
    with st.form("fixed_rule_form", clear_on_submit=True):
        name = st.text_input("항목명", placeholder="예: 통신비")
        category = st.selectbox("카테고리", EXPENSE_CATS, index=2)  # 기본 생활
        amount_str = st.text_input("금액", placeholder="예: 55,000")
        memo = st.text_input("메모", placeholder="예: KT / 매달")
        ok = st.form_submit_button("규칙 저장", type="primary")
    if ok and run_once("fixed_rule_add"):
        amt = to_int_amount(amount_str)
        if not name.strip():
            st.error("항목명을 입력해 주세요.")
        elif amt is None or amt <= 0:
            st.error("금액은 1 이상으로 입력해 주세요.")
        else:
            row = {
                "id": str(uuid.uuid4()),
                "name": name.strip(),
                "category": category,
                "amount": amt,
                "memo": memo.strip(),
                "created_at": now_str(),
            }
            safe_append_rows("fixed_rules", [row])
            read_df.clear()
            st.success("규칙 저장 완료")

    rules = read_df("fixed_rules")
    rules = ensure_cols(rules, ["id", "name", "category", "amount", "memo"])
    if not rules.empty:
        rules["amount"] = pd.to_numeric(rules["amount"], errors="coerce").fillna(0).astype(int)

    st.markdown("#### 등록된 규칙")
    if rules.empty:
        st.info("등록된 규칙이 없습니다.")
    else:
        v = rules[["name", "category", "amount", "memo"]].rename(columns={"name": "항목", "category": "카테고리", "amount": "금액", "memo": "메모"})
        st.dataframe(
            v.style.format({"금액": lambda x: fmt_amount(int(x))})
            .set_properties(subset=["금액"], **{"text-align": "right"}),
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("#### 2) 월에 고정지출 반영하기")
    c1, c2 = st.columns(2)
    with c1:
        fy = st.selectbox("연도", list(range(today.year - 3, today.year + 2)), index=3, key="fy")
    with c2:
        fm = st.selectbox("월", list(range(1, 13)), index=today.month - 1, key="fm")
    fym = ym_from_year_month(fy, fm)
    if st.button("선택 월에 반영", type="primary", disabled=rules.empty) and run_once("fixed_apply"):
        applied_rows = []
        ledger_rows = []

        for _, r in rules.iterrows():
            nm = str(r.get("name", "")).strip()
            cat = str(r.get("category", "생활")).strip() or "생활"
            amt = int(r.get("amount", 0))
            mm = str(r.get("memo", "")).strip()

            dk = f"{fym}|{nm}|{amt}"
            applied_rows.append({
                "id": str(uuid.uuid4()),
                "ym": fym,
                "name": nm,
                "amount": amt,
                "memo": mm,
                "created_at": now_str(),
                "dedup_key": dk,
            })

            # ledger에도 dedup_key 가능하면 사용
            ldk = f"FIX|{fym}|{nm}|{amt}"
            ledger_rows.append({
                "id": str(uuid.uuid4()),
                "ym": fym,
                "day": "01일",
                "type": "지출",
                "category": cat,
                "amount": amt,
                "memo": f"[고정] {nm} {mm}".strip(),
                "created_at": now_str(),
                "dedup_key": ldk,
            })

        wrote, skipped = safe_append_rows("fixed_applied", applied_rows, dedup_key_field="dedup_key")

        # fixed_applied에서 중복 걸러졌더라도, ledger가 dedup_key를 갖고 있으면 ledger에서도 중복이 걸러짐
        if wrote:
            safe_append_rows("ledger", ledger_rows, dedup_key_field="dedup_key")
            read_df.clear()
            st.success(f"{wrote}건 반영 완료 (중복 {skipped}건 제외)")
        else:
            read_df.clear()
            st.info(f"이미 해당 월에 동일 고정지출이 반영되어 있습니다. (중복 {skipped}건)")

    applied = read_df("fixed_applied")
    applied = ensure_cols(applied, ["ym", "name", "amount", "memo"])
    a = applied[applied["ym"].astype(str) == fym].copy() if not applied.empty else applied.iloc[0:0]
    st.markdown("#### 해당 월 반영 내역")
    if a.empty:
        st.info("해당 월에 반영된 고정지출이 없습니다.")
    else:
        a["amount"] = pd.to_numeric(a["amount"], errors="coerce").fillna(0).astype(int)
        v = a[["name", "amount", "memo"]].rename(columns={"name": "항목", "amount": "금액", "memo": "메모"})
        st.dataframe(
            v.style.format({"금액": lambda x: fmt_amount(int(x))})
            .set_properties(subset=["금액"], **{"text-align": "right"}),
            use_container_width=True,
            hide_index=True,
        )

# =============================
# 4) 경조사비 / 5) 제로페이
# =============================
def simple_inout_tab(sheet_key: str, title: str):
    st.subheader(title)

    st.markdown("#### 내역 입력")
    t = st.selectbox("구분", ["수입", "지출"], key=f"{sheet_key}_type")

    with st.form(f"{sheet_key}_form", clear_on_submit=True):
        date_str = st.text_input("날짜 (0000/00/00)", value=today.strftime("%Y/%m/%d"), key=f"{sheet_key}_date")
        amt_str = st.text_input("금액", value="", key=f"{sheet_key}_amt")
        memo = st.text_input("메모", value="", key=f"{sheet_key}_memo")
        ok = st.form_submit_button("저장", type="primary")
    if ok and run_once(f"{sheet_key}_add"):
        parsed = parse_yyyy_mm_dd(date_str)
        amt = to_int_amount(amt_str)
        if not parsed:
            st.error("날짜 형식을 확인해 주세요. 예: 2026/03/03")
        elif amt is None:
            st.error("금액 형식을 확인해 주세요.")
        else:
            _, _, d = parsed
            row = {
                "id": str(uuid.uuid4()),
                "day": day_to_korean(d),
                "type": t,
                "amount": amt,
                "memo": memo.strip(),
                "created_at": now_str(),
            }
            safe_append_rows(sheet_key, [row])
            read_df.clear()
            st.success("저장 완료")

    df = read_df(sheet_key)
    df = ensure_cols(df, ["day", "type", "amount", "memo"])
    if df.empty:
        st.info("내역이 없습니다.")
        return

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0).astype(int)
    inc = int(df.loc[df["type"] == "수입", "amount"].sum())
    exp = int(df.loc[df["type"] == "지출", "amount"].sum())
    diff = inc - exp

    c1, c2, c3 = st.columns(3)
    c1.metric("전체 수입합계", fmt_amount(inc))
    c2.metric("전체 지출합계", fmt_amount(exp))
    c3.metric("차액", fmt_amount(diff))

    st.markdown("#### 전체 내역")
    show = df[["day", "type", "amount", "memo"]].rename(columns={"day": "날짜", "type": "구분", "amount": "금액", "memo": "메모"})
    st.dataframe(
        show.style
        .format({"금액": lambda x: fmt_amount(int(x))})
        .applymap(lambda v: "color:#d32f2f;" if isinstance(v, (int, float)) and v < 0 else "", subset=["금액"])
        .set_properties(subset=["금액"], **{"text-align": "right"}),
        use_container_width=True,
        hide_index=True,
    )

with tabs[3]:
    simple_inout_tab("events", "경조사비")

with tabs[4]:
    simple_inout_tab("zeropay", "제로페이")

# =============================
# 6) 신용카드
# =============================
with tabs[5]:
    st.subheader("신용카드")

    st.markdown("#### 1) 카드 혜택 정리")
    with st.form("card_form", clear_on_submit=True):
        card_name = st.text_input("카드명", placeholder="예: 현대카드 M")
        benefit = st.text_area("혜택 메모", height=80, placeholder="예: 대중교통 10% ...")
        ok = st.form_submit_button("저장", type="primary")
    if ok and run_once("card_add"):
        if not card_name.strip():
            st.error("카드명을 입력해 주세요.")
        else:
            row = {
                "id": str(uuid.uuid4()),
                "card_name": card_name.strip(),
                "benefit_memo": benefit.strip(),
                "created_at": now_str(),
            }
            safe_append_rows("cards", [row])
            read_df.clear()
            st.success("저장 완료")

    cards = read_df("cards")
    cards = ensure_cols(cards, ["card_name", "benefit_memo"])
    card_list = sorted(list(set(cards["card_name"].astype(str).tolist()))) if not cards.empty else []

    if not card_list:
        st.info("등록된 카드가 없습니다.")
    else:
        st.markdown("##### 카드 목록")
        view = cards[["card_name", "benefit_memo"]].rename(columns={"card_name": "카드명", "benefit_memo": "혜택 메모"})
        st.dataframe(view, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("#### 2) 정기결제 관리")
    with st.form("sub_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            sub_card = st.selectbox("카드", card_list if card_list else ["(카드 먼저 등록)"], disabled=not bool(card_list))
        with c2:
            merchant = st.text_input("가맹점/서비스", placeholder="예: 넷플릭스")
        a1, a2 = st.columns(2)
        with a1:
            amt_str = st.text_input("금액", placeholder="예: 13,500")
        with a2:
            billing_day = st.selectbox("결제일(일)", list(range(1, 32)))
        memo = st.text_input("메모", placeholder="예: 프리미엄")
        ok2 = st.form_submit_button("정기결제 저장", type="primary", disabled=not bool(card_list))
    if ok2 and run_once("sub_add"):
        amt = to_int_amount(amt_str)
        if not merchant.strip():
            st.error("가맹점/서비스명을 입력해 주세요.")
        elif amt is None or amt <= 0:
            st.error("금액은 1 이상으로 입력해 주세요.")
        else:
            row = {
                "id": str(uuid.uuid4()),
                "card_name": sub_card,
                "merchant": merchant.strip(),
                "amount": amt,
                "billing_day": int(billing_day),
                "memo": memo.strip(),
                "created_at": now_str(),
            }
            safe_append_rows("subscriptions", [row])
            read_df.clear()
            st.success("저장 완료")

    subs = read_df("subscriptions")
    subs = ensure_cols(subs, ["card_name", "merchant", "amount", "billing_day", "memo"])
    if subs.empty:
        st.info("정기결제 내역이 없습니다.")
    else:
        subs["amount"] = pd.to_numeric(subs["amount"], errors="coerce").fillna(0).astype(int)

        st.markdown("#### 3) 카드별 정기결제 총액")
        total_by_card = subs.groupby("card_name")["amount"].sum().reset_index().rename(columns={"card_name": "카드명", "amount": "총액"})
        st.dataframe(
            total_by_card.style.format({"총액": lambda x: fmt_amount(int(x))})
            .set_properties(subset=["총액"], **{"text-align": "right"}),
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("#### 4) 카드 선택 → 정기결제 내역")
        sel = st.selectbox("카드 선택", sorted(subs["card_name"].astype(str).unique().tolist()))
        sub_view = subs[subs["card_name"].astype(str) == sel].copy()
        sub_view = sub_view[["merchant", "amount", "billing_day", "memo"]].rename(
            columns={"merchant": "가맹점/서비스", "amount": "금액", "billing_day": "결제일", "memo": "메모"}
        )
        st.dataframe(
            sub_view.style.format({"금액": lambda x: fmt_amount(int(x))})
            .set_properties(subset=["금액"], **{"text-align": "right"}),
            use_container_width=True,
            hide_index=True,
        )
