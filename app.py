import re
import time
import random
import uuid
from datetime import datetime, date as dt_date

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

import gspread
from google.oauth2.service_account import Credentials

# =============================
# Page / iOS PWA meta
# =============================
st.set_page_config(page_title="가계부", layout="centered", initial_sidebar_state="collapsed")
st.session_state['_nonce'] = st.session_state.get('_nonce', 0) + 1

components.html(
    """
<script>
  (function(){
    const head = document.getElementsByTagName('head')[0];
    const metaViewport = document.createElement('meta');
    metaViewport.name = "viewport";
    metaViewport.content = "width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no, viewport-fit=cover";
    head.appendChild(metaViewport);

    const metaTheme = document.createElement('meta');
    metaTheme.name = "theme-color";
    metaTheme.content = "#0e1117";
    head.appendChild(metaTheme);
  })();
</script>
""",
    height=0,
)

# =============================
# Style (Dark bg, white inputs/cards)
# =============================
st.markdown(
    """
<style>
button, input, textarea {font-size: 16px !important;} /* iOS zoom 방지 */

html, body, [data-testid="stAppViewContainer"], [data-testid="stHeader"], [data-testid="stApp"] {
  background: #0e1117 !important;
}

.block-container {padding-top: 0.9rem; padding-bottom: 3rem; max-width: 900px;}
@media (max-width: 480px) {
  .block-container{max-width:100% !important; padding-left:0.85rem !important; padding-right:0.85rem !important; padding-top:0.6rem !important;}
}

h1, h2, h3 {color: #f3f4f6 !important;}
p, label, span {color: #e5e7eb !important;}
small {color:#cbd5e1 !important;}

/* Sticky tabs */
div[data-testid="stTabs"]{position: sticky; top: 0; z-index: 999; background: #0e1117; padding-top: 0.2rem; border-bottom: 1px solid rgba(255,255,255,0.08);}
div[data-testid="stTabs"] button {padding: 10px 12px; color:#e5e7eb !important;}
div[data-testid="stTabs"] [data-baseweb="tab-list"]{
  overflow-x:auto !important; flex-wrap:nowrap !important; -webkit-overflow-scrolling:touch !important;
}

/* White card for forms */
[data-testid="stForm"]{
  background:#ffffff !important;
  padding: 14px !important;
  border-radius: 14px !important;
  border: 1px solid rgba(0,0,0,0.06);
}
[data-testid="stForm"] *{ color:#111827 !important; }

/* Inputs white */
input, textarea {background:#ffffff !important; color:#111827 !important; border-radius:10px !important;}
[data-baseweb="select"] > div {background:#ffffff !important; color:#111827 !important; border-radius:10px !important;}
[data-baseweb="input"] > div {background:#ffffff !important; border-radius:10px !important;}
[data-baseweb="datepicker"] > div {background:#ffffff !important; border-radius:10px !important;}

.stButton>button {border-radius: 12px;}

/* Metric row (force 3 cards in one line on iPhone) */
.metric-row {display:flex; gap:10px; flex-wrap:nowrap; width:100%; align-items:stretch;}
.metric-card {flex:1 1 0; min-width:0; background:#111827; border:1px solid rgba(255,255,255,0.08);
  border-radius:14px; padding:12px 12px;}
.metric-title {font-size:12px; color:#cbd5e1; margin:0 0 6px 0; line-height:1.1;}
.metric-value {font-size:18px; color:#f3f4f6; font-weight:700; margin:0; text-align:right; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;}
@media (max-width:480px){
  .metric-row{gap:8px;}
  .metric-card{padding:10px;}
  .metric-title{font-size:11px;}
  .metric-value{font-size:16px;}
}

</style>
""",
    unsafe_allow_html=True,
)

# =============================
# Constants
# =============================
INCOME_CATS = ["월급", "부수입", "이자", "캐시백", "기타"]
EXPENSE_CATS = ["식재료", "외식/배달", "생활", "육아", "여가", "교통/유류", "의료", "기타"]
TAB_NAMES = ["가계부", "예산", "고정지출", "경조사비", "제로페이", "신용카드", "자산관리"]

SHEET_NAMES = {
    "ledger": "ledger",
    "budgets": "budgets",
    "fixed_rules": "fixed_rules",
    "fixed_applied": "fixed_applied",
    "events": "events",
    "zeropay": "zeropay",
    "cards": "cards",
    "subscriptions": "subscriptions",

    # Assets
    "assets_bank": "assets_bank",
    "assets_cash": "assets_cash",
    "assets_other": "assets_other",
}

# =============================

def apply_fixed_to_month(ym: str):
    """Apply fixed_rules into fixed_applied and ledger for a given ym.
    Prevents duplicates using dedup_key.
    """
    rules = ensure_cols(read_df("fixed_rules"), ["id","name","amount","memo","created_at"])
    if rules.empty:
        return False, "고정지출 규칙이 없습니다."
    applied = ensure_cols(read_df("fixed_applied"), ["id","ym","day","name","amount","memo","created_at","dedup_key"])
    ledger = ensure_cols(read_df("ledger"), ["id","ym","day","type","category","amount","memo","created_at","dedup_key"])
    # normalize
    rules["amount"] = pd.to_numeric(rules["amount"], errors="coerce").fillna(0).astype(int)
    if not applied.empty:
        existing_applied = set(applied["dedup_key"].astype(str).tolist())
    else:
        existing_applied = set()
    if not ledger.empty:
        existing_ledger = set(ledger["dedup_key"].astype(str).tolist())
    else:
        existing_ledger = set()

    created = 0
    for _, r in rules.iterrows():
        name = str(r.get("name","")).strip()
        amt = int(r.get("amount",0))
        memo = str(r.get("memo","")).strip()
        day = "01일"
        dk_applied = f"FIX_APPLIED|{ym}|{day}|{name}|{amt}"
        dk_ledger = f"FIX_LEDGER|{ym}|{day}|{name}|{amt}"
        if dk_applied not in existing_applied:
            append_row(
                "fixed_applied",
                {
                    "ym": ym,
                    "day": day,
                    "name": name,
                    "amount": amt,
                    "memo": memo,
                    "dedup_key": dk_applied,
                },
            )
            existing_applied.add(dk_applied)
        if dk_ledger not in existing_ledger:
            append_row(
                "ledger",
                {
                    "ym": ym,
                    "day": day,
                    "type": "지출",
                    "category": "고정지출",
                    "amount": amt,
                    "memo": f"[고정] {name} {memo}".strip(),
                    "dedup_key": dk_ledger,
                },
            )
            existing_ledger.add(dk_ledger)
            created += 1

    return True, f"{ym} 고정지출 반영 완료 ({created}건)"

# Helpers
# =============================
def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def append_row(sheet_key: str, data: dict):
    """Append a row to a sheet using header order. Auto-fills id/created_at if columns exist."""
    h = headers(sheet_key)
    row = []
    for col in h:
        if col == "id":
            row.append(str(data.get("id") or uuid.uuid4()))
        elif col == "created_at":
            row.append(str(data.get("created_at") or now_str()))
        else:
            v = data.get(col, "")
            row.append("" if v is None else str(v))
    w = ws(sheet_key)
    with_retry(lambda: w.append_row(row, value_input_option="USER_ENTERED"))
    return True

def ym_from(y: int, m: int) -> str:
    return f"{y:04d}-{m:02d}"

def day_k(d: int) -> str:
    return f"{d:02d}일"

def fmt_amount(n: int) -> str:
    return f"{int(n):,}"

def metrics_row(income_sum: int, expense_sum: int, shown_balance: int):
    """Render 3 metrics in a single horizontal row (works reliably on iPhone PWA)."""
    st.markdown(
        f"""
<div class="metric-row">
  <div class="metric-card">
    <p class="metric-title">수입합계</p>
    <p class="metric-value">{fmt_amount(income_sum)}</p>
  </div>
  <div class="metric-card">
    <p class="metric-title">지출합계</p>
    <p class="metric-value">{fmt_amount(expense_sum)}</p>
  </div>
  <div class="metric-card">
    <p class="metric-title">잔액</p>
    <p class="metric-value">{fmt_amount(shown_balance)}</p>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

def metrics_row3(t1: str, v1: int, t2: str, v2: int, t3: str, v3: int):
    st.markdown(
        f"""
<div class="metric-row">
  <div class="metric-card">
    <p class="metric-title">{t1}</p>
    <p class="metric-value">{fmt_amount(v1)}</p>
  </div>
  <div class="metric-card">
    <p class="metric-title">{t2}</p>
    <p class="metric-value">{fmt_amount(v2)}</p>
  </div>
  <div class="metric-card">
    <p class="metric-title">{t3}</p>
    <p class="metric-value">{fmt_amount(v3)}</p>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

def to_int_amount(s: str):
    s = str(s).strip().replace(",", "")
    if not re.fullmatch(r"-?\d+", s):
        return None
    try:
        return int(s)
    except:
        return None

def cat_order_key(cat: str) -> int:
    try:
        return EXPENSE_CATS.index(cat)
    except ValueError:
        return 999

def month_filter(df: pd.DataFrame, ym: str) -> pd.DataFrame:
    if df.empty or "ym" not in df.columns:
        return df.iloc[0:0]
    return df[df["ym"].astype(str) == ym].copy()

def with_retry(fn, tries=6, base_sleep=0.7):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            last = e
            time.sleep(base_sleep * (2 ** i) + random.uniform(0, 0.25))
    raise last

def ensure_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df

def col_to_a1(col_idx_1based: int) -> str:
    n = col_idx_1based
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

def run_once(key: str) -> bool:
    """Allow an action once per *interaction*, not once per app lifetime.
    Uses an incrementing nonce that changes on each rerun where this function is called.
    """
    nonce = st.session_state.get("_nonce", 0)
    last = st.session_state.get(f"_once_{key}")
    if last == nonce:
        return False
    st.session_state[f"_once_{key}"] = nonce
    return True

# =============================
# Google Sheets
# =============================
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

@st.cache_resource
def ws(sheet_key: str):

    title = SHEET_NAMES[sheet_key]

    try:
        return gs_book().worksheet(title)

    except gspread.exceptions.WorksheetNotFound:

        new_ws = gs_book().add_worksheet(
            title=title,
            rows=1000,
            cols=20
        )

        headers_map = {
            "assets_bank": [
                "id","bank_name","account_number","join_date",
                "balance","asset_type","maturity_date",
                "memo","created_at"
            ],

            "assets_cash": [
                "id","cash_type","amount","trade_datetime",
                "detail","memo","created_at"
            ],

            "assets_other": [
                "id","content","datetime","amount",
                "memo","created_at"
            ],
        }

        if sheet_key in headers_map:
            new_ws.append_row(headers_map[sheet_key])

        return new_ws

@st.cache_data(ttl=600)
def read_df(sheet_key: str) -> pd.DataFrame:
    w = ws(sheet_key)
    rows = with_retry(lambda: w.get_all_records())
    return pd.DataFrame(rows)

def headers(sheet_key: str) -> list[str]:
    return with_retry(lambda: ws(sheet_key).row_values(1))

def safe_append_rows(sheet_key: str, rows: list[dict], dedup_key_field: str | None = None):
    if not rows:
        return 0, 0
    w = ws(sheet_key)
    h = headers(sheet_key)
    if not h:
        raise RuntimeError(f"{sheet_key}: header row is empty")

    existing = set()
    if dedup_key_field and (dedup_key_field in h):
        c = h.index(dedup_key_field) + 1
        a1 = col_to_a1(c)
        col_vals = with_retry(lambda: w.get(f"{a1}2:{a1}"))
        existing = set([r[0] for r in col_vals if r and r[0]])

    to_write = []
    skipped = 0
    for r in rows:
        if dedup_key_field and (dedup_key_field in h):
            k = str(r.get(dedup_key_field, "")).strip()
            if k and k in existing:
                skipped += 1
                continue
            if k:
                existing.add(k)
        to_write.append([r.get(x, "") for x in h])

    if not to_write:
        return 0, skipped

    with_retry(lambda: w.append_rows(to_write, value_input_option="USER_ENTERED"))
    return len(to_write), skipped

# =============================
# Common UI
# =============================
st.title("📒 가계부")
mobile_mode = st.toggle("📱 모바일 모드", value=True)

if st.button("🔄 데이터 새로고침"):
    read_df.clear()
    st.success("새로고침 완료")

tabs = st.tabs(TAB_NAMES)
today = datetime.now().date()

def month_picker(prefix: str):
    years = list(range(today.year - 3, today.year + 2))
    months = list(range(1, 13))
    if mobile_mode:
        y = st.selectbox("연도", years, index=years.index(today.year), key=f"{prefix}_y")
        m = st.selectbox("월", months, index=today.month - 1, key=f"{prefix}_m")
    else:
        c1, c2 = st.columns(2)
        with c1:
            y = st.selectbox("연도", years, index=years.index(today.year), key=f"{prefix}_y")
        with c2:
            m = st.selectbox("월", months, index=today.month - 1, key=f"{prefix}_m")
    return y, m, ym_from(y, m)

# =============================
# Tab 1: Ledger
# =============================
with tabs[0]:
    st.subheader("가계부")
    y, m, ym = month_picker("ledger")

    st.markdown("#### 내역 입력")
    entry_type = st.selectbox("구분", ["수입", "지출"], key="ledger_type")
    cats = INCOME_CATS if entry_type == "수입" else EXPENSE_CATS
    if st.session_state.get("ledger_last_type") != entry_type:
        st.session_state["ledger_category"] = cats[0]
        st.session_state["ledger_last_type"] = entry_type

    with st.form("ledger_add", clear_on_submit=True):
        picked = st.date_input("날짜", value=today)
        category = st.selectbox("카테고리", cats, key="ledger_category")
        amount_str = st.text_input("금액 (예: 12,345 / -12,345)", value="")
        memo = st.text_input("메모", value="")
        ok = st.form_submit_button("저장", type="primary")

    if ok and run_once("ledger_add_once"):
        amt = to_int_amount(amount_str)
        if amt is None:
            st.error("금액 형식을 확인해 주세요.")
        else:
            row_ym = ym_from(picked.year, picked.month)
            row_day = day_k(picked.day)
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
            wrote, _ = safe_append_rows("ledger", [row], dedup_key_field="dedup_key")
            read_df.clear()
            if wrote:
                st.success("저장 완료")
            else:
                st.info("동일 내용이 이미 있어 추가하지 않았습니다.")

    ledger = ensure_cols(read_df("ledger"), ["id","ym","day","type","category","amount","memo","created_at","dedup_key"])
    if not ledger.empty:
        ledger["amount"] = pd.to_numeric(ledger["amount"], errors="coerce").fillna(0).astype(int)
    this_month = month_filter(ledger, ym) if not ledger.empty else ledger.iloc[0:0]

    st.markdown("#### 월 요약")
    income_sum = int(this_month.loc[this_month["type"] == "수입", "amount"].sum()) if not this_month.empty else 0
    expense_sum = int(this_month.loc[this_month["type"] == "지출", "amount"].sum()) if not this_month.empty else 0
    balance_month = income_sum - expense_sum

    y_prefix = f"{y:04d}-"
    ytd = ledger[ledger["ym"].astype(str).str.startswith(y_prefix)].copy() if not ledger.empty else ledger.iloc[0:0]
    if not ytd.empty:
        ytd = ytd[ytd["ym"].astype(str) <= ym].copy()
    ytd_income = int(ytd.loc[ytd["type"] == "수입", "amount"].sum()) if not ytd.empty else 0
    ytd_expense = int(ytd.loc[ytd["type"] == "지출", "amount"].sum()) if not ytd.empty else 0
    balance_ytd = ytd_income - ytd_expense

    bmode = st.radio("잔액 보기", ["선택 월 기준", "당해년도 월 누적"], horizontal=True)
    shown_balance = balance_month if bmode == "선택 월 기준" else balance_ytd
    metrics_row(income_sum, expense_sum, shown_balance)

    st.markdown("#### 예산현황")
    budgets = ensure_cols(read_df("budgets"), ["id","ym","category","target","created_at","dedup_key"])
    budgets_m = budgets[budgets["ym"].astype(str) == ym].copy() if not budgets.empty else budgets.iloc[0:0]
    if budgets_m.empty:
        st.info("선택한 월의 예산이 없습니다. 예산 탭에서 설정해 주세요.")
    else:
        budgets_m["target"] = pd.to_numeric(budgets_m["target"], errors="coerce").fillna(0).astype(int)
        exp = this_month[(this_month["type"]=="지출") & (this_month["category"].isin(EXPENSE_CATS))].copy()
        exp_by_cat = exp.groupby("category")["amount"].sum().to_dict() if not exp.empty else {}
        view = pd.DataFrame({"카테고리": budgets_m["category"].astype(str), "목표금액": budgets_m["target"].astype(int)})
        view["실제지출금액"] = view["카테고리"].map(lambda c: int(exp_by_cat.get(c, 0)))
        view["차액"] = view["목표금액"] - view["실제지출금액"]
        view["__ord"] = view["카테고리"].map(cat_order_key)
        view = view.sort_values(["__ord", "카테고리"]).drop(columns="__ord")
        view = view[["카테고리","목표금액","실제지출금액","차액"]]
        st.dataframe(
            view.style
            .format({c: (lambda x: fmt_amount(int(x))) for c in ["목표금액","실제지출금액","차액"]})
            .map(lambda v: "color:#ef4444;" if isinstance(v,(int,float)) and v < 0 else "", subset=["차액"])
            .set_properties(subset=["목표금액","실제지출금액","차액"], **{"text-align":"right"}),
            use_container_width=True,
            hide_index=True,
        )

        # --- 예산 요약 (합계/지출/잔액) ---
        budget_total = int(view["목표금액"].sum())
        budget_spent = int(view["실제지출금액"].sum())
        budget_balance = budget_total - budget_spent
        metrics_row3("예산합계", budget_total, "예산지출합계", budget_spent, "잔액", budget_balance)

    st.markdown("#### 전체내역")

    if this_month.empty:

        st.info("선택한 월에 내역이 없습니다.")

    else:

        view = this_month.copy()

        view["amount"] = pd.to_numeric(view["amount"], errors="coerce").fillna(0).astype(int)

        # 수입/지출 모두 표시, created_at(등록시각)은 표시하지 않음

        show = view[["day","type","category","amount","memo"]].rename(

            columns={"day":"날짜","type":"구분","category":"카테고리","amount":"금액","memo":"메모"}

        )

        st.dataframe(

            show.style.format({"금액": lambda x: fmt_amount(int(x))})

                .set_properties(subset=["금액"], **{"text-align":"right"}),

            use_container_width=True,

            hide_index=True,

        )

# =============================
# Tab 2: Budgets
# =============================
with tabs[1]:
    st.subheader("예산 설정")
    st.caption("저장을 두 번 눌러도 동일 월/카테고리/목표금액은 한 번만 반영됩니다.")
    by, bm, bym = month_picker("budget")

    with st.form("budget_add", clear_on_submit=True):
        cat = st.selectbox("지출 카테고리", EXPENSE_CATS)
        target_str = st.text_input("목표금액", placeholder="예: 300,000")
        ok = st.form_submit_button("저장하기", type="primary")

    if ok and run_once("budget_add_once"):
        t = to_int_amount(target_str)
        if t is None or t < 0:
            st.error("목표금액은 0 이상의 숫자로 입력해 주세요.")
        else:
            dk = f"BUDGET|{bym}|{cat}|{t}"
            row = {"id":str(uuid.uuid4()),"ym":bym,"category":cat,"target":t,"created_at":now_str(),"dedup_key":dk}
            wrote, _ = safe_append_rows("budgets", [row], dedup_key_field="dedup_key")
            read_df.clear()
            if wrote:
                st.success("저장 완료")
            else:
                st.info("동일 내용이 이미 있어 추가하지 않았습니다.")

    budgets = ensure_cols(read_df("budgets"), ["id","ym","category","target","created_at","dedup_key"])
    b = budgets[budgets["ym"].astype(str)==bym].copy() if not budgets.empty else budgets.iloc[0:0]
    if b.empty:
        st.info("해당 월 예산이 없습니다.")
    else:
        b["target"] = pd.to_numeric(b["target"], errors="coerce").fillna(0).astype(int)
        view = b[["category","target"]].rename(columns={"category":"카테고리","target":"목표금액"})
        view["__ord"] = view["카테고리"].map(cat_order_key)
        view = view.sort_values(["__ord","카테고리"]).drop(columns="__ord")
        st.markdown("#### 예산 목록")
        st.dataframe(view.style.format({"목표금액": lambda x: fmt_amount(int(x))}).set_properties(subset=["목표금액"], **{"text-align":"right"}), use_container_width=True, hide_index=True)

# =============================
# Tab 3: Fixed (category fixed)
# =============================
with tabs[2]:
    st.subheader("고정지출")
    st.caption("카테고리는 자동으로 '고정지출'로 저장됩니다.")

    # 반영할 월 선택
    fy, fm, fym = month_picker("fixed")
    if st.button("📌 선택한 월에 고정지출 반영", type="primary", key="apply_fixed_btn"):
        ok, msg = apply_fixed_to_month(fym)
        if ok:
            read_df.clear()
            st.success(msg)
        else:
            st.error(msg)

    st.markdown("#### 내역")
    with st.form("fixed_rule_add", clear_on_submit=True):
        name = st.text_input("항목명", placeholder="예: 통신비")
        st.text_input("카테고리", value="고정지출", disabled=True)
        amount_str = st.text_input("금액", placeholder="예: 55,000")
        memo = st.text_input("메모", placeholder="예: KT / 매달")
        ok = st.form_submit_button("저장", type="primary")
    if ok and run_once("fixed_rule_add_once"):
        amt = to_int_amount(amount_str)
        if not name.strip():
            st.error("항목명을 입력해 주세요.")
        elif amt is None or amt <= 0:
            st.error("금액은 1 이상으로 입력해 주세요.")
        else:
            row = {"id":str(uuid.uuid4()),"name":name.strip(),"amount":amt,"memo":memo.strip(),"created_at":now_str()}
            safe_append_rows("fixed_rules", [row])
            read_df.clear()
            st.success("저장 완료")

    rules = ensure_cols(read_df("fixed_rules"), ["id","name","amount","memo","created_at"])
    if not rules.empty:
        rules["amount"] = pd.to_numeric(rules["amount"], errors="coerce").fillna(0).astype(int)

    st.markdown("#### 고정지출 내역")
    if rules.empty:
        st.info("등록된 내역이 없습니다.")
    else:
        view = rules[["name","amount","memo"]].rename(columns={"name":"항목","amount":"금액","memo":"메모"})
        st.dataframe(view.style.format({"금액": lambda x: fmt_amount(int(x))}).set_properties(subset=["금액"], **{"text-align":"right"}), use_container_width=True, hide_index=True)

# =============================
# Tabs 4-5: Events / Zeropay (edit/delete)
# =============================
def inout_tab(sheet_key: str, title: str):
    st.subheader(title)
    st.markdown("#### 내역 입력")
    t = st.selectbox("구분", ["수입", "지출"], key=f"{sheet_key}_type")

    with st.form(f"{sheet_key}_add", clear_on_submit=True):
        d = st.date_input("날짜", value=today, key=f"{sheet_key}_date")
        amt_str = st.text_input("금액", value="", key=f"{sheet_key}_amt")
        memo = st.text_input("메모", value="", key=f"{sheet_key}_memo")
        ok = st.form_submit_button("저장", type="primary")

    if ok and run_once(f"{sheet_key}_add_once"):
        amt = to_int_amount(amt_str)
        if amt is None:
            st.error("금액 형식을 확인해 주세요.")
        else:
            row = {"id":str(uuid.uuid4()),"day":day_k(d.day),"type":t,"amount":amt,"memo":memo.strip(),"created_at":now_str()}
            safe_append_rows(sheet_key, [row])
            read_df.clear()
            st.success("저장 완료")

    df = ensure_cols(read_df(sheet_key), ["id","day","type","amount","memo","created_at"])
    if df.empty:
        st.info("내역이 없습니다.")
        return

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0).astype(int)
    inc = int(df.loc[df["type"]=="수입","amount"].sum())
    exp = int(df.loc[df["type"]=="지출","amount"].sum())
    diff = inc - exp

    metrics_row3("전체 수입합계", inc, "전체 지출합계", exp, "차액", diff)

    st.markdown("#### 전체 내역")
    show = df[["day","type","amount","memo"]].rename(columns={"day":"날짜","type":"구분","amount":"금액","memo":"메모"})
    st.dataframe(
        show.style.format({"금액": lambda x: fmt_amount(int(x))})
        .map(lambda v: "color:#ef4444;" if isinstance(v,(int,float)) and v < 0 else "", subset=["금액"])
        .set_properties(subset=["금액"], **{"text-align":"right"}),
        use_container_width=True,
        hide_index=True,
    )

# =============================
# Tab 4: Events
# =============================
with tabs[3]:
    inout_tab("events", "경조사비")

# =============================
# Tab 5: Zeropay
# =============================
with tabs[4]:
    inout_tab("zeropay", "제로페이")

# =============================
# Tab 6: Cards (edit/delete)
# =============================
with tabs[5]:
    st.subheader("신용카드")

    st.markdown("#### 카드 혜택 정리")
    with st.form("card_add", clear_on_submit=True):
        card_name = st.text_input("카드명", placeholder="예: 현대카드 M")
        benefit = st.text_area("혜택 메모", height=80, placeholder="예: 대중교통 10% ...")
        ok = st.form_submit_button("저장", type="primary")
    if ok and run_once("card_add_once"):
        if not card_name.strip():
            st.error("카드명을 입력해 주세요.")
        else:
            row = {"id":str(uuid.uuid4()),"card_name":card_name.strip(),"benefit_memo":benefit.strip(),"created_at":now_str()}
            safe_append_rows("cards", [row])
            read_df.clear()
            st.success("저장 완료")

    cards = ensure_cols(read_df("cards"), ["id","card_name","benefit_memo","created_at"])
    card_list = sorted([c for c in cards["card_name"].astype(str).tolist() if c]) if not cards.empty else []

    if cards.empty:
        st.info("등록된 카드가 없습니다.")
    else:
        st.markdown("##### 카드 목록")
        view = cards[["card_name","benefit_memo"]].rename(columns={"card_name":"카드명","benefit_memo":"혜택 메모"})
        st.dataframe(view, use_container_width=True, hide_index=True)

        subs = ensure_cols(read_df("subscriptions"), ["id","card_name","merchant","amount","billing_day","memo","created_at"])
        if not subs.empty:
            subs["amount"] = pd.to_numeric(subs["amount"], errors="coerce").fillna(0).astype(int)

        st.markdown("#### 카드별 정기결제 내역")
        if subs.empty:
            st.info("정기결제 내역이 없습니다.")
        else:
            sel = st.selectbox("카드 선택", sorted(subs["card_name"].astype(str).unique().tolist()), key="sub_card_pick")
            sub_view = subs[subs["card_name"].astype(str) == sel].copy()
            sub_total = int(sub_view["amount"].sum())
            
            st.markdown(
                f"### 💳 정기결제 총액: {fmt_amount(sub_total)}원"
            )
            show = sub_view[["merchant","amount","billing_day","memo"]].rename(columns={"merchant":"가맹점/서비스","amount":"금액","billing_day":"결제일","memo":"메모"})
            st.dataframe(
                show.style.format({"금액": lambda x: fmt_amount(int(x))}).set_properties(subset=["금액"], **{"text-align":"right"}),
                use_container_width=True,
                hide_index=True,
            )
            
            st.markdown("#### 정기결제 수정 / 삭제")
            
            edit_target = st.selectbox(
                "수정할 항목 선택",
                sub_view["merchant"].astype(str) + " / " + sub_view["billing_day"].astype(str) + "일",
                key="sub_edit_select"
            )
            
            selected_row = sub_view[
                (
                    sub_view["merchant"].astype(str)
                    + " / "
                    + sub_view["billing_day"].astype(str)
                    + "일"
                ) == edit_target
            ].iloc[0]
            
            with st.form("subscription_edit_form"):

                new_merchant = st.text_input(
                    "가맹점/서비스",
                    value=str(selected_row["merchant"])
                )
                
                new_amount = st.text_input(
                    "금액",
                    value=str(selected_row["amount"])
                )
                
                new_day = st.text_input(
                    "결제일",
                    value=str(selected_row["billing_day"])
                )
                new_memo = st.text_input(
                    "메모",
                    value=str(selected_row["memo"])
                )
                
                c1, c2 = st.columns(2)
                
                with c1:
                    update_ok = st.form_submit_button(
                        "수정 저장",
                        type="primary"
                    )
                with c2:
                    delete_ok = st.form_submit_button(
                        "삭제"
                    )
                    
            if update_ok:
                
                amt = to_int_amount(new_amount)
                
                if amt is None:
                    st.error("금액 형식을 확인해 주세요.")
                
                else:
                    target_id = str(selected_row["id"])
                    
                    subs["id"] = subs["id"].astype(str)

                    subs = subs.fillna("")
                    
                    subs.loc[
                        subs["id"] == target_id,
                        ["merchant","amount","billing_day","memo"]
                    ] = [
                        new_merchant.strip(),
                        amt,
                        new_day.strip(),
                        new_memo.strip()
                    ]

                    st.write(
                        subs.loc[subs["id"] == target_id]
                    )
                    
                    ws("subscriptions").clear()
                    
                    ws("subscriptions").append_row([
                        "id",
                        "card_name",
                        "merchant",
                        "amount",
                        "billing_day",
                        "memo",
                        "created_at"
                    ])
                    
                    rows = []
                    
                    for _, r in subs.iterrows():
                        rows.append([
                            r.get("id", ""),
                            r.get("card_name", ""),
                            r.get("merchant", ""),
                            r.get("amount", ""),
                            r.get("billing_day", ""),
                            r.get("memo", ""),
                            r.get("created_at", ""),
                        ])
                    
                    if rows:
                        ws("subscriptions").append_rows(
                            rows,
                            value_input_option="USER_ENTERED"
                        )
            
                    st.cache_data.clear()
                    
                    st.success("수정 완료")
                    
                    st.rerun()
                    
            if delete_ok:
                
                remain = subs[
                    subs["id"] != selected_row["id"]
                ]
                
                ws("subscriptions").clear()
                
                ws("subscriptions").append_row([
                    "id",
                    "card_name",
                    "merchant",
                    "amount",
                    "billing_day",
                    "memo",
                    "created_at"
                ])
                
                if not remain.empty:
                    
                    safe_append_rows(
                        "subscriptions",
                        remain.to_dict("records")
                    )
                
                read_df.clear()
                
                st.success("삭제 완료")
                
                st.rerun()

# =============================
# Tab 7: Assets
# =============================
with tabs[6]:
    st.subheader("자산관리")

    asset_tab = st.radio(
        "구분",
        ["통장", "현금", "기타"],
        horizontal=True
    )

    # =============================
    # 통장
    # =============================
    if asset_tab == "통장":

        st.markdown("#### 통장 등록")

        with st.form("bank_asset_add", clear_on_submit=True):

            bank_name = st.text_input("은행명")
            account_number = st.text_input("계좌번호")
            join_date = st.date_input(
                "가입일",
                value=today,
                min_value=datetime(1990, 1, 1).date(),
                max_value=today
            )
            balance_str = st.text_input("잔액")
            asset_type = st.selectbox("상품구분", ["입출금", "예금", "적금", "청약", "기타"])
            has_maturity = st.checkbox("만기일 있음", value=False)
            
            maturity_date = None
            
            if has_maturity:
                maturity_date = st.date_input(
                    "만기일",
                    value=today,
                    min_value=datetime(1970, 1, 1).date()
                )
                
            memo = st.text_input("기타 정보")

            ok = st.form_submit_button("저장", type="primary")

        if ok and run_once("bank_asset_once"):

            balance = to_int_amount(balance_str)

            if balance is None:
                st.error("잔액 형식을 확인해 주세요.")

            else:

                row = {
                    "id": str(uuid.uuid4()),
                    "bank_name": bank_name.strip(),
                    "account_number": account_number.strip(),
                    "join_date": str(join_date),
                    "balance": balance,
                    "asset_type": asset_type,
                    "maturity_date": str(maturity_date) if maturity_date else "",
                    "memo": memo.strip(),
                    "created_at": now_str(),
                }

                safe_append_rows("assets_bank", [row])

                read_df.clear()

                st.success("저장 완료")

        bank_df = ensure_cols(
            read_df("assets_bank"),
            ["id","bank_name","account_number","join_date","balance","asset_type","maturity_date","memo"]
        )

        if not bank_df.empty:

            bank_df["balance"] = pd.to_numeric(
                bank_df["balance"],
                errors="coerce"
            ).fillna(0).astype(int)

            total_bank = int(bank_df["balance"].sum())

            metrics_row3(
                "통장 개수",
                len(bank_df),
                "통장 잔액 합계",
                total_bank,
                "평균 잔액",
                int(total_bank / max(len(bank_df), 1))
            )

            filter_type = st.selectbox(
                "상품구분 필터",
                ["전체"] + sorted(bank_df["asset_type"].astype(str).unique().tolist())
            )

            view_df = bank_df.copy()

            if filter_type != "전체":
                view_df = view_df[view_df["asset_type"] == filter_type]

            show = view_df[
                ["bank_name","account_number","asset_type","join_date","maturity_date","balance","memo"]
            ].rename(
                columns={
                    "bank_name":"은행명",
                    "account_number":"계좌번호",
                    "asset_type":"상품구분",
                    "join_date":"가입일",
                    "maturity_date":"만기일",
                    "balance":"잔액",
                    "memo":"기타정보",
                }
            )

            st.dataframe(
                show.style.format({
                    "잔액": lambda x: fmt_amount(int(x))
                }),
                use_container_width=True,
                hide_index=True,
            )

            st.markdown("#### 삭제")

            delete_target = st.selectbox(
                "삭제할 통장 선택",
                view_df["bank_name"].astype(str) + " / " + view_df["account_number"].astype(str),
                key="bank_delete_select"
            )

            if st.button("통장 삭제", key="bank_delete_btn"):

                idx = (
                    view_df["bank_name"].astype(str) + " / " + view_df["account_number"].astype(str)
                ) == delete_target

                delete_id = view_df[idx]["id"].iloc[0]

                remain = bank_df[bank_df["id"] != delete_id]

                ws("assets_bank").clear()
                ws("assets_bank").append_row(
                    ["id","bank_name","account_number","join_date","balance","asset_type","maturity_date","memo","created_at"]
                )

                rows = remain.to_dict("records")

                if rows:
                    safe_append_rows("assets_bank", rows)

                read_df.clear()
                st.success("삭제 완료")
                st.rerun()

    # =============================
    # 현금
    # =============================
    elif asset_tab == "현금":

        st.markdown("#### 현금 내역")

        with st.form("cash_asset_add", clear_on_submit=True):

            cash_type = st.selectbox(
                "구분",
                ["빌린 돈", "빌려준 돈"]
            )

            amount_str = st.text_input("금액")

            trade_datetime = st.date_input(
                "거래일시",
                value=today
            )

            detail = st.text_input("거래내역")

            memo = st.text_input("기타 정보")

            ok = st.form_submit_button("저장", type="primary")

        if ok and run_once("cash_asset_once"):

            amount = to_int_amount(amount_str)

            if amount is None:
                st.error("금액 형식을 확인해 주세요.")

            else:

                row = {
                    "id": str(uuid.uuid4()),
                    "cash_type": cash_type,
                    "amount": amount,
                    "trade_datetime": str(trade_datetime),
                    "detail": detail.strip(),
                    "memo": memo.strip(),
                    "created_at": now_str(),
                }

                safe_append_rows("assets_cash", [row])

                read_df.clear()

                st.success("저장 완료")

        cash_df = ensure_cols(
            read_df("assets_cash"),
            ["id","cash_type","amount","trade_datetime","detail","memo"]
        )

        if not cash_df.empty:

            cash_df["amount"] = pd.to_numeric(
                cash_df["amount"],
                errors="coerce"
            ).fillna(0).astype(int)

            borrowed = int(
                cash_df.loc[cash_df["cash_type"] == "빌린 돈", "amount"].sum()
            )

            lent = int(
                cash_df.loc[cash_df["cash_type"] == "빌려준 돈", "amount"].sum()
            )

            net_cash = lent - borrowed

            metrics_row3(
                "빌린 돈",
                borrowed,
                "빌려준 돈",
                lent,
                "순 현금자산",
                net_cash
            )

            show = cash_df[
                ["cash_type","amount","trade_datetime","detail","memo"]
            ].rename(
                columns={
                    "cash_type":"구분",
                    "amount":"금액",
                    "trade_datetime":"거래일시",
                    "detail":"거래내역",
                    "memo":"기타정보",
                }
            )

            st.dataframe(
                show.style.format({
                    "금액": lambda x: fmt_amount(int(x))
                }),
                use_container_width=True,
                hide_index=True,
            )

    # =============================
    # 기타
    # =============================
    else:

        st.markdown("#### 기타 자산")

        with st.form("other_asset_add", clear_on_submit=True):

            content = st.text_input("내용")

            dt = st.date_input(
                "일시",
                value=today
            )

            amount_str = st.text_input("금액")

            memo = st.text_input("기타 정보")

            ok = st.form_submit_button("저장", type="primary")

        if ok and run_once("other_asset_once"):

            amount = to_int_amount(amount_str)

            if amount is None:
                st.error("금액 형식을 확인해 주세요.")

            else:

                row = {
                    "id": str(uuid.uuid4()),
                    "content": content.strip(),
                    "datetime": str(dt),
                    "amount": amount,
                    "memo": memo.strip(),
                    "created_at": now_str(),
                }

                safe_append_rows("assets_other", [row])

                read_df.clear()

                st.success("저장 완료")

        other_df = ensure_cols(
            read_df("assets_other"),
            ["id","content","datetime","amount","memo"]
        )

        if not other_df.empty:

            other_df["amount"] = pd.to_numeric(
                other_df["amount"],
                errors="coerce"
            ).fillna(0).astype(int)

            show = other_df[
                ["content","datetime","amount","memo"]
            ].rename(
                columns={
                    "content":"내용",
                    "datetime":"일시",
                    "amount":"금액",
                    "memo":"기타정보",
                }
            )

            st.dataframe(
                show.style.format({
                    "금액": lambda x: fmt_amount(int(x))
                }),
                use_container_width=True,
                hide_index=True,
            )
