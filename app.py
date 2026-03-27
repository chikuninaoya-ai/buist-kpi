import streamlit as st
import gspread
import pandas as pd
import re
import unicodedata
import altair as alt
from google.oauth2.service_account import Credentials

# ── 設定 ──
SHEET1_ID = "1PRtTE9qhjNOfz1_3GoNu397-_zN33FRmgkd5C_5Mbrg"
SHEET2_ID = "1-jGXpUpUrfIHLbBuzeydGdYYr9v_J6QKynJKtLC5L7w"
TARGET_YEAR_MONTH = "2026-03"
COOLOFF_COL = "クーリングオフ\n（発生時のみ記載）"
PAYMENT_COL = "翌月末\n着金額"

st.set_page_config(page_title="ブイストKPI管理", page_icon="📊", layout="wide")

# ── グローバルCSS: メトリックカードを枠線付きに ──
st.markdown("""
<style>
[data-testid="stMetric"] {
    background: #ffffff;
    border: 1px solid #e0e0e0;
    border-radius: 10px;
    padding: 14px 18px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
}
[data-testid="stMetric"] label {
    color: #666 !important;
    font-size: 13px !important;
}
[data-testid="stMetric"] [data-testid="stMetricValue"] {
    font-size: 26px !important;
    font-weight: 700 !important;
}
div[data-testid="stHorizontalBlock"] > div {
    padding: 0 4px;
}
/* カスタム指標カード */
.kpi-card {
    background: #ffffff;
    border: 1px solid #e0e0e0;
    border-radius: 10px;
    padding: 14px 18px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
}
.kpi-card .kpi-label {
    color: #666;
    font-size: 13px;
    margin-bottom: 4px;
}
.kpi-card .kpi-values {
    font-size: 14px;
    line-height: 1.6;
}
.kpi-card .kpi-actual-label {
    color: #888;
    font-size: 12px;
}
.kpi-card .kpi-actual {
    font-size: 22px;
    font-weight: 700;
    color: #111;
}
.kpi-card .kpi-target-label {
    color: #aaa;
    font-size: 12px;
    margin-left: 8px;
}
.kpi-card .kpi-target {
    color: #999;
    font-size: 16px;
    font-weight: 500;
    margin-left: 2px;
}
</style>
""", unsafe_allow_html=True)


# ── ユーティリティ ──

def extract_cr(name):
    m = re.search(r"CR(\d+(?:-\d+)?)", name, re.IGNORECASE)
    return f"CR{m.group(1)}" if m else None


def extract_cr_detail(name):
    """CR番号＋バリアント名を抽出（例: CR27-2作り直し, CR18re, CR27-2背景変更）"""
    m = re.search(r"CR(\d+(?:-\d+)?[a-zA-Z]*)([\u3000-\u9FFF\uF900-\uFAFF]*)", name, re.IGNORECASE)
    if not m:
        return None
    return f"CR{m.group(1)}{m.group(2)}" if m.group(2) else f"CR{m.group(1)}"


def short_camp_name(name):
    """キャンペーン名を短縮（例: CR27-2_tCPA → 27-2, 【テスト】CR27-2作り直し_tCPA → 27-2作り直し）"""
    m = re.search(r"CR(\d+(?:-\d+)?[a-zA-Z]*)([\u3000-\u9FFF\uF900-\uFAFF]*)", name, re.IGNORECASE)
    if not m:
        return name[:20]
    base = m.group(1) + m.group(2)
    if "コピー" in name:
        base += "コピー"
    return base


def normalize_name(name):
    if not name:
        return ""
    name = unicodedata.normalize("NFKC", name)
    name = "".join(c for c in name if unicodedata.category(c)[0] not in ("S", "C") or c == " ")
    name = re.sub(r"[®™©*ﾟ゚°·•◡̈?]", "", name)
    return name.strip().lower()


def parse_price(price_str):
    if not price_str:
        return 0
    cleaned = re.sub(r"[¥￥,、\s]", "", str(price_str))
    try:
        return int(cleaned)
    except ValueError:
        return 0


def format_yen(val):
    if val == 0:
        return "-"
    return f"¥{val:,.0f}"


def cr_sort_key(x):
    m = re.search(r"\d+", x)
    return int(m.group()) if m else 0


def classify_channel(raw):
    s = str(raw).strip()
    if "セールスマーケ" in s:
        return "セールスマーケ"
    if s == "SNS":
        return "SNS"
    if "Meta" in s or s.startswith("広告"):
        return "Meta"
    return "その他"


# ── データ取得 ──

@st.cache_data(ttl=300)
def load_data():
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=scopes
    )
    gc = gspread.authorize(creds)

    sh1 = gc.open_by_key(SHEET1_ID)
    sh2 = gc.open_by_key(SHEET2_ID)

    # ── 1. CR別予算 ──
    ws_budget = sh1.worksheet("CR別予算")
    budget_rows = ws_budget.get_all_values()
    df_budget = pd.DataFrame(budget_rows[1:], columns=budget_rows[0])
    df_budget = df_budget[df_budget["Day"].str.startswith(TARGET_YEAR_MONTH)]
    df_budget["CR番号"] = df_budget["Campaign Name"].apply(extract_cr)
    df_budget["CR詳細"] = df_budget["Campaign Name"].apply(extract_cr_detail)
    df_budget = df_budget.dropna(subset=["CR番号"])
    df_budget["金額"] = df_budget["Amount Spent"].str.replace(",", "").astype(float)

    budget_by_cr = df_budget.groupby("CR番号")["金額"].sum().reset_index()
    budget_by_cr.columns = ["CR番号", "消化予算"]
    total_budget = int(budget_by_cr["消化予算"].sum())

    # ── 2. リスト流入経路 ──
    ws_list = sh1.worksheet("リスト流入経路")
    list_rows = ws_list.get_all_values()
    df_list = pd.DataFrame(list_rows[1:], columns=list_rows[0])
    df_list = df_list[df_list["配信基準日時"].str.startswith(TARGET_YEAR_MONTH)]
    df_list["CR番号"] = df_list["登録経路"].apply(extract_cr)
    df_list["CR詳細"] = df_list["登録経路"].apply(extract_cr_detail)
    df_list = df_list.dropna(subset=["CR番号"])
    df_list["LINE名_正規化"] = df_list["LINE登録名"].apply(normalize_name)
    df_list["登録日"] = df_list["配信基準日時"].str[:10]

    # CR内でユニークなLINE登録名のみカウント（同一人物の重複除外）
    registrations_by_cr = df_list.groupby("CR番号")["LINE登録名"].nunique().reset_index()
    registrations_by_cr.columns = ["CR番号", "LINE登録数"]
    # 全体ユニーク数（同一人物が複数CRに登録していても1人としてカウント）
    total_unique_registrations = int(df_list["LINE登録名"].nunique())

    # ── 3. 仮成約者リスト ──
    ws_sales = sh2.worksheet("仮成約者リスト")
    sales_rows = ws_sales.get_all_values()
    sales_header = sales_rows[0]

    march_start = None
    next_section = None
    for i, row in enumerate(sales_rows):
        if row[0] and "2026年3月" in row[0]:
            march_start = i + 1
        elif march_start and row[0] and re.match(r"20\d{2}年\d{1,2}月", row[0]):
            next_section = i
            break

    end = next_section if next_section else len(sales_rows)
    march_data = sales_rows[march_start:end] if march_start else []

    df_sales = pd.DataFrame(march_data, columns=sales_header)
    df_sales = df_sales[df_sales["LINE名"].str.strip() != ""]
    df_sales = df_sales[df_sales["LINE名"].notna()]

    # クーリングオフ除外
    if COOLOFF_COL in df_sales.columns:
        cooloff_mask = df_sales[COOLOFF_COL].str.strip() != ""
        df_cooloff = df_sales[cooloff_mask].copy()
        df_sales = df_sales[~cooloff_mask]
    else:
        df_cooloff = pd.DataFrame()

    # D列（チャネル）
    col_source = [c for c in df_sales.columns if "広告" in c or "SNS" in c][0]

    df_all_sales = df_sales.copy()
    df_all_sales["受注金額"] = df_all_sales["受注単価"].apply(parse_price)
    df_all_sales["LINE名_正規化"] = df_all_sales["LINE名"].apply(normalize_name)
    df_all_sales["チャネル"] = df_all_sales[col_source].apply(classify_channel)

    # 翌月末着金額（S列）
    if PAYMENT_COL in df_all_sales.columns:
        df_all_sales["着金額"] = df_all_sales[PAYMENT_COL].apply(parse_price)
    else:
        df_all_sales["着金額"] = 0

    # セールス担当列
    sales_rep_col = [c for c in df_all_sales.columns if "セールス" in c and "担当" in c]
    if sales_rep_col:
        df_all_sales["セールス担当"] = df_all_sales[sales_rep_col[0]].str.strip()

    # ── 4. LINE名照合 → CR番号・CR詳細付与 ──
    line_to_cr = {}
    line_to_cr_detail = {}
    for _, row in df_list.iterrows():
        key = normalize_name(row["LINE登録名"])
        if key:
            line_to_cr[key] = row["CR番号"]
            line_to_cr_detail[key] = row["CR詳細"]

    df_all_sales["CR番号"] = df_all_sales["LINE名_正規化"].map(line_to_cr)
    df_all_sales["CR詳細"] = df_all_sales["LINE名_正規化"].map(line_to_cr_detail)

    list_names = list(line_to_cr.keys())
    for idx in df_all_sales[df_all_sales["CR番号"].isna()].index:
        sale_name = df_all_sales.at[idx, "LINE名_正規化"]
        if not sale_name or len(sale_name) < 2:
            continue
        for list_name in list_names:
            if not list_name or len(list_name) < 2:
                continue
            shorter = min(len(sale_name), len(list_name))
            if shorter < 3:
                continue
            if sale_name in list_name or list_name in sale_name:
                df_all_sales.at[idx, "CR番号"] = line_to_cr[list_name]
                df_all_sales.at[idx, "CR詳細"] = line_to_cr_detail[list_name]
                break

    # ── 5. 個別相談ステータス（成約率算出） ──
    # 広告
    ws_consult_ad = sh2.worksheet("個別相談ステータス(広告)")
    ad_rows = ws_consult_ad.get_all_values()
    consult_ad = []
    for row in ad_rows[2:]:
        if row[0].strip().startswith("2026/03") or row[0].strip().startswith("2026/3"):
            status = row[7].strip() if len(row) > 7 else ""
            route = row[5].strip() if len(row) > 5 else ""
            line_name = row[1].strip() if len(row) > 1 else ""
            if status:
                date_str = row[0].strip()[:10]
                try:
                    date_normalized = str(pd.Timestamp(date_str).date())
                except Exception:
                    date_normalized = ""
                # LINE名照合でCR詳細を紐づけ（売上と同じロジック）
                cr_detail = None
                if line_name:
                    norm_name = normalize_name(line_name)
                    cr_detail = line_to_cr_detail.get(norm_name)
                    if not cr_detail and norm_name and len(norm_name) >= 2:
                        for ln, cd in line_to_cr_detail.items():
                            if not ln or len(ln) < 2 or min(len(norm_name), len(ln)) < 3:
                                continue
                            if norm_name in ln or ln in norm_name:
                                cr_detail = cd
                                break
                consult_ad.append({
                    "ステータス": status, "経路": route,
                    "日付": date_normalized,
                    "CR詳細": cr_detail,
                })
    df_consult_ad = pd.DataFrame(consult_ad) if consult_ad else pd.DataFrame(columns=["ステータス", "経路", "日付", "CR詳細"])

    # SNS（A=申し込み日, D=LINE名, H=ステータス）
    ws_consult_sns = sh2.worksheet("個別相談ステータス(SNS)")
    sns_rows = ws_consult_sns.get_all_values()
    consult_sns = []
    for row in sns_rows[2:]:
        if row[0].strip().startswith("2026/03") or row[0].strip().startswith("2026/3"):
            status = row[7].strip() if len(row) > 7 else ""
            if status:
                consult_sns.append({"ステータス": status})
    df_consult_sns = pd.DataFrame(consult_sns) if consult_sns else pd.DataFrame(columns=["ステータス"])

    # ── 6. KPI目標値 ──
    def _parse_num(s):
        if not s:
            return 0
        s = re.sub(r"[¥￥,%、\s]", "", str(s))
        try:
            return float(s)
        except ValueError:
            return 0

    # 広告KPI Meta
    kpi_meta = {}
    try:
        ws_kpi_ad = sh2.worksheet("【広告】KPI｜Meta")
        kpi_rows = ws_kpi_ad.get_all_values()
        kpi_start = None
        for i, row in enumerate(kpi_rows):
            if row[0] and "2026年3月" in row[0]:
                kpi_start = i
                break
        if kpi_start:
            r = lambda off: kpi_rows[kpi_start + off] if kpi_start + off < len(kpi_rows) else []
            kpi_meta = {
                "消化予算": _parse_num(r(2)[2]),
                "アポ獲得数": _parse_num(r(2)[8]),
                "成約数": _parse_num(r(2)[14]),
                "成約率": _parse_num(r(6)[14]),
                "許容CPA": _parse_num(r(7)[2]),
                "リスト獲得数": _parse_num(r(11)[2]),
                "アポ実施数": _parse_num(r(11)[8]),
                "アポ着席率": _parse_num(r(15)[11]),
            }
    except Exception:
        pass

    # SNS KPI
    kpi_sns = {}
    try:
        ws_kpi_sns = sh2.worksheet("【SNS】KPI")
        kpi_rows_s = ws_kpi_sns.get_all_values()
        kpi_start_s = None
        for i, row in enumerate(kpi_rows_s):
            if row[0] and "3月" in row[0] and "26" in row[0]:
                kpi_start_s = i
                break
        if kpi_start_s:
            r = lambda off: kpi_rows_s[kpi_start_s + off] if kpi_start_s + off < len(kpi_rows_s) else []
            kpi_sns = {
                "受注高": _parse_num(r(4)[2]),
                "リスト獲得数": _parse_num(r(4)[11]),
                "必要アポ数": _parse_num(r(7)[8]),
                "アポ着席率": _parse_num(r(11)[5]),
                "成約数": _parse_num(r(16)[2]),
                "成約率": _parse_num(r(19)[2]),
            }
    except Exception:
        pass

    # 日別登録数（広告ダッシュボード用）- CR詳細内で重複除外
    df_list_dedup = df_list.drop_duplicates(subset=["LINE登録名", "CR詳細"], keep="first")
    regs_by_day_cr = df_list_dedup.groupby(["登録日", "CR番号", "CR詳細"]).size().reset_index(name="登録数")

    return (budget_by_cr, registrations_by_cr, df_all_sales, df_cooloff,
            total_budget, df_consult_ad, df_consult_sns, kpi_meta, kpi_sns,
            df_budget, regs_by_day_cr, total_unique_registrations)


def calc_close_rate(df_consult):
    if len(df_consult) == 0:
        return 0, 0, 0, 0.0
    won = len(df_consult[df_consult["ステータス"] == "受注"])
    lost = len(df_consult[df_consult["ステータス"] == "失注"])
    chasing = len(df_consult[df_consult["ステータス"] == "追いかけ"])
    denom = won + lost + chasing  # KPI準拠: 成約/(成約+失注+追いかけ)
    rate = round(won / denom * 100, 1) if denom > 0 else 0.0
    return won, lost, denom, rate


def calc_consultation_stats(df_consult):
    if len(df_consult) == 0:
        return {
            "アポ獲得数": 0, "実施済み": 0, "成約数": 0, "失注数": 0,
            "追いかけ数": 0, "キャンセル数": 0, "相談飛び数": 0,
            "実施待ち": 0, "日程調整中": 0, "審査落ち": 0, "着席率": 0.0,
        }
    s = df_consult["ステータス"]
    won = int((s == "受注").sum())
    lost = int((s == "失注").sum())
    chasing = int((s == "追いかけ").sum())
    cancel = int(s.str.contains("キャンセル", na=False).sum())
    no_show = int(s.str.contains("相談飛び", na=False).sum())
    waiting = int(s.str.contains("日程確定", na=False).sum())
    adjusting = int(s.str.contains("日程調整", na=False).sum())
    screening = int(s.str.contains("審査落ち", na=False).sum())

    # KPI準拠: 着席済み = 成約+失注+追いかけ（相談実施済み全件）
    seated = won + lost + chasing
    total = len(df_consult)
    apo_acquired = total - screening - adjusting  # 審査落ち・日程調整中はアポ確定前のため除外
    attend_denom = seated + cancel + no_show
    attend_rate = round(seated / attend_denom * 100, 1) if attend_denom > 0 else 0.0

    return {
        "アポ獲得数": apo_acquired, "実施済み": seated,
        "成約数": won, "失注数": lost,
        "追いかけ数": chasing, "キャンセル数": cancel, "相談飛び数": no_show,
        "実施待ち": waiting, "日程調整中": adjusting, "審査落ち": screening,
        "着席率": attend_rate,
    }


# ── 描画 ──

def build_cr_table(budget_by_cr, registrations_by_cr, matched_sales):
    if len(matched_sales) == 0:
        sales_by_cr = pd.DataFrame(columns=["CR番号", "成約件数", "売上合計"])
    else:
        sales_by_cr = matched_sales.groupby("CR番号").agg(
            成約件数=("受注金額", "count"),
            売上合計=("受注金額", "sum"),
        ).reset_index()

    all_crs = set(budget_by_cr["CR番号"]) | set(registrations_by_cr["CR番号"])
    if len(sales_by_cr) > 0:
        all_crs |= set(sales_by_cr["CR番号"])

    df = pd.DataFrame({"CR番号": sorted(all_crs, key=cr_sort_key)})
    df = df.merge(budget_by_cr, on="CR番号", how="left")
    df = df.merge(registrations_by_cr, on="CR番号", how="left")
    df = df.merge(sales_by_cr, on="CR番号", how="left")
    df = df.fillna(0)

    for col in ["消化予算", "LINE登録数", "成約件数", "売上合計"]:
        df[col] = df[col].astype(int)

    df["CPA（登録）"] = df.apply(
        lambda r: int(r["消化予算"] / r["LINE登録数"]) if r["LINE登録数"] > 0 else 0, axis=1
    )
    df["CPA（成約）"] = df.apply(
        lambda r: int(r["消化予算"] / r["成約件数"]) if r["成約件数"] > 0 else 0, axis=1
    )
    df["ROAS"] = df.apply(
        lambda r: round(r["売上合計"] / r["消化予算"] * 100, 1) if r["消化予算"] > 0 else 0, axis=1
    )
    return df


def _kpi_card(label, actual, target=None):
    """1行で「実績: ○○  目標: ○○」を表示するHTMLカード"""
    target_html = ""
    if target:
        target_html = f'<span class="kpi-target-label">目標:</span><span class="kpi-target">{target}</span>'
    return (
        f'<div class="kpi-card">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-values">'
        f'<span class="kpi-actual-label">実績:</span><span class="kpi-actual">{actual}</span>'
        f'{target_html}'
        f'</div>'
        f'</div>'
    )


def render_summary(sales_df, budget=None, total_registrations=None,
                   close_rate_data=None, show_list_cpa=False, targets=None):
    t = targets or {}
    total_cv = len(sales_df)
    total_sales = int(sales_df["受注金額"].sum()) if total_cv > 0 else 0
    total_payment = int(sales_df["着金額"].sum()) if "着金額" in sales_df.columns and total_cv > 0 else 0

    if close_rate_data:
        won, lost, consult_total, rate = close_rate_data
    else:
        won, lost, consult_total, rate = 0, 0, 0, 0.0

    if budget and budget > 0:
        tgt_budget = int(t.get("消化予算", 0))
        tgt_cv = int(t.get("成約数", 0))
        tgt_sales = int(t.get("受注高", 0))
        row1 = st.columns(4)
        row1[0].markdown(_kpi_card("消化予算", format_yen(budget),
                                    format_yen(tgt_budget) if tgt_budget else None), unsafe_allow_html=True)
        row1[1].markdown(_kpi_card("成約件数", f"{total_cv}件",
                                    f"{tgt_cv}件" if tgt_cv else None), unsafe_allow_html=True)
        row1[2].markdown(_kpi_card("売上", format_yen(total_sales),
                                    format_yen(tgt_sales) if tgt_sales else None), unsafe_allow_html=True)
        row1[3].markdown(_kpi_card("翌月末着金額", format_yen(total_payment)), unsafe_allow_html=True)

        row2_items = []
        if show_list_cpa:
            list_cpa = int(budget / total_registrations) if total_registrations and total_registrations > 0 else 0
            tgt_cpa = int(t.get("許容CPA", 0))
            row2_items.append(("リスト獲得CPA", format_yen(list_cpa),
                               format_yen(tgt_cpa) if tgt_cpa else None))
            tgt_list = int(t.get("リスト獲得数", 0))
            row2_items.append(("リスト獲得数", f"{total_registrations}件" if total_registrations else "-",
                               f"{tgt_list}件" if tgt_list else None))
        cpa = int(budget / total_cv) if total_cv > 0 else 0
        row2_items.append(("CPA（成約）", format_yen(cpa), None))
        roas = round(total_sales / budget * 100, 1) if budget > 0 else 0
        row2_items.append(("ROAS（売上）", f"{roas}%", None))
        roas_payment = round(total_payment / budget * 100, 1) if budget > 0 else 0
        row2_items.append(("ROAS（着金）", f"{roas_payment}%", None))
        tgt_rate = t.get("成約率", 0)
        if consult_total > 0:
            row2_items.append(("成約率", f"{rate}%", f"{tgt_rate:.0f}%" if tgt_rate else None))
        else:
            row2_items.append(("成約率", "-", f"{tgt_rate:.0f}%" if tgt_rate else None))

        row2 = st.columns(len(row2_items))
        for i, (label, value, target_text) in enumerate(row2_items):
            row2[i].markdown(_kpi_card(label, value, target_text), unsafe_allow_html=True)
    else:
        tgt_cv = int(t.get("成約数", 0))
        tgt_sales = int(t.get("受注高", 0))
        row1 = st.columns(4)
        row1[0].markdown(_kpi_card("成約件数", f"{total_cv}件",
                                    f"{tgt_cv}件" if tgt_cv else None), unsafe_allow_html=True)
        row1[1].markdown(_kpi_card("売上", format_yen(total_sales),
                                    format_yen(tgt_sales) if tgt_sales else None), unsafe_allow_html=True)
        row1[2].markdown(_kpi_card("翌月末着金額", format_yen(total_payment)), unsafe_allow_html=True)
        avg = int(total_sales / total_cv) if total_cv > 0 else 0
        row1[3].markdown(_kpi_card("平均単価", format_yen(avg)), unsafe_allow_html=True)

        tgt_rate = t.get("成約率", 0)
        if consult_total > 0:
            row2 = st.columns(4)
            row2[0].markdown(_kpi_card("成約率", f"{rate}%",
                                        f"{tgt_rate:.0f}%" if tgt_rate else None), unsafe_allow_html=True)


def render_consultation_stats(stats, targets=None, actual_sales_count=None):
    if stats["アポ獲得数"] == 0:
        return
    t = targets or {}
    # 成約数は仮成約者リストの件数で上書き（個別相談ステータスの受注≠実売上）
    if actual_sales_count is not None:
        stats = dict(stats)  # コピーして元データを壊さない
        stats["成約数"] = actual_sales_count

    st.markdown("##### 個別相談数値")
    tgt_apo = int(t.get("アポ獲得数", 0))
    tgt_impl = int(t.get("アポ実施数", 0))
    tgt_cv = int(t.get("成約数", 0))
    tgt_attend = t.get("アポ着席率", 0)

    row1 = st.columns(5)
    row1[0].markdown(_kpi_card("アポ獲得数", f"{stats['アポ獲得数']}件",
                                f"{tgt_apo}件" if tgt_apo else None), unsafe_allow_html=True)
    row1[1].markdown(_kpi_card("アポ実施済み", f"{stats['実施済み']}件",
                                f"{tgt_impl}件" if tgt_impl else None), unsafe_allow_html=True)
    row1[2].markdown(_kpi_card("成約数", f"{stats['成約数']}件",
                                f"{tgt_cv}件" if tgt_cv else None), unsafe_allow_html=True)
    row1[3].markdown(_kpi_card("失注数", f"{stats['失注数']}件"), unsafe_allow_html=True)
    row1[4].markdown(_kpi_card("アポ着席率", f"{stats['着席率']}%",
                                f"{tgt_attend:.0f}%" if tgt_attend else None), unsafe_allow_html=True)

    row2 = st.columns(5)
    row2[0].markdown(_kpi_card("追いかけ", f"{stats['追いかけ数']}件"), unsafe_allow_html=True)
    row2[1].markdown(_kpi_card("未着席キャンセル", f"{stats['キャンセル数']}件"), unsafe_allow_html=True)
    row2[2].markdown(_kpi_card("未着席 相談飛び", f"{stats['相談飛び数']}件"), unsafe_allow_html=True)
    row2[3].markdown(_kpi_card("今月 実施待ち", f"{stats['実施待ち']}件"), unsafe_allow_html=True)
    row2[4].markdown(_kpi_card("日程調整中", f"{stats['日程調整中']}件"), unsafe_allow_html=True)


def render_cr_chart(df_cr):
    chart_df = df_cr[df_cr["消化予算"] > 0].copy()
    if len(chart_df) == 0:
        return

    cr_order = chart_df["CR番号"].tolist()

    melted = chart_df[["CR番号", "消化予算", "売上合計"]].melt(
        id_vars="CR番号", var_name="項目", value_name="金額"
    )
    melted["項目"] = melted["項目"].map({"消化予算": "広告費", "売上合計": "売上"})

    chart = (
        alt.Chart(melted)
        .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("CR番号:N", sort=cr_order, title=None,
                     axis=alt.Axis(labelAngle=0, labelFontSize=12)),
            y=alt.Y("金額:Q", title=None,
                     axis=alt.Axis(format="~s", labelFontSize=11)),
            xOffset=alt.XOffset("項目:N"),
            color=alt.Color(
                "項目:N",
                scale=alt.Scale(
                    domain=["売上", "広告費"],
                    range=["#4CAF50", "#E53935"],
                ),
                legend=alt.Legend(title=None, orient="bottom", labelFontSize=13),
            ),
            tooltip=[
                alt.Tooltip("CR番号:N"),
                alt.Tooltip("項目:N"),
                alt.Tooltip("金額:Q", format=",.0f"),
            ],
        )
        .properties(height=400)
        .configure_view(strokeWidth=0)
    )
    st.altair_chart(chart, use_container_width=True)


def render_cr_table(df_master):
    display_df = df_master.copy()
    display_df["消化予算"] = display_df["消化予算"].apply(format_yen)
    display_df["売上合計"] = display_df["売上合計"].apply(format_yen)
    display_df["CPA（登録）"] = display_df["CPA（登録）"].apply(format_yen)
    display_df["CPA（成約）"] = display_df["CPA（成約）"].apply(format_yen)
    display_df["ROAS"] = display_df["ROAS"].apply(lambda x: f"{x}%" if x > 0 else "-")
    display_df["LINE登録数"] = display_df["LINE登録数"].apply(lambda x: f"{x:,}件" if x > 0 else "-")
    display_df["成約件数"] = display_df["成約件数"].apply(lambda x: f"{x:,}件" if x > 0 else "-")

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "CR番号": st.column_config.TextColumn("CR番号", width="small"),
            "消化予算": st.column_config.TextColumn("消化予算", width="medium"),
            "LINE登録数": st.column_config.TextColumn("LINE登録数", width="small"),
            "成約件数": st.column_config.TextColumn("成約件数", width="small"),
            "売上合計": st.column_config.TextColumn("売上合計", width="medium"),
            "CPA（登録）": st.column_config.TextColumn("CPA（登録）", width="medium"),
            "CPA（成約）": st.column_config.TextColumn("CPA（成約）", width="medium"),
            "ROAS": st.column_config.TextColumn("ROAS", width="small"),
        },
    )


def render_sales_list(sales_df, show_cr=False):
    if len(sales_df) == 0:
        st.info("成約者データなし")
        return
    cols = []
    if show_cr:
        cols.append("CR番号")
    cols += ["LINE名", "本名", "商品名", "受注単価"]
    if "セールス担当" in sales_df.columns:
        cols.append("セールス担当")
    available = [c for c in cols if c in sales_df.columns]
    st.dataframe(sales_df[available], use_container_width=True, hide_index=True)


# ── メイン ──

def main():
    st.title("📊 ブイストKPI管理")
    st.caption("2026年3月 | Google Sheets 自動連携")

    with st.spinner("Google Sheets からデータを読み込み中..."):
        (budget_by_cr, registrations_by_cr, df_all_sales, df_cooloff,
         total_budget, df_consult_ad, df_consult_sns, kpi_meta, kpi_sns,
         df_budget_raw, regs_by_day_cr, total_unique_registrations) = load_data()

    if len(df_cooloff) > 0:
        st.info(f"クーリングオフ・キャンセル・退会: {len(df_cooloff)}件を売上から除外済み")

    # チャネル別に分割
    df_meta = df_all_sales[df_all_sales["チャネル"] == "Meta"]
    df_sns = df_all_sales[df_all_sales["チャネル"] == "SNS"]
    df_sm = df_all_sales[df_all_sales["チャネル"] == "セールスマーケ"]

    total_registrations = total_unique_registrations

    # 成約率算出
    # KPI準拠: Meta KPIは個別相談ステータス(広告)全件を使う（経路フィルタなし）
    close_meta = calc_close_rate(df_consult_ad)
    close_sns = calc_close_rate(df_consult_sns)
    df_consult_all = pd.concat([df_consult_ad, df_consult_sns], ignore_index=True)
    close_all = calc_close_rate(df_consult_all)

    # 個別相談数値指標
    consult_stats_all = calc_consultation_stats(df_consult_all)
    consult_stats_meta = calc_consultation_stats(df_consult_ad)  # 広告全件=Meta KPI
    consult_stats_sns = calc_consultation_stats(df_consult_sns)
    # セールスマーケ = 広告シートのMeta以外の経路
    df_consult_sm = df_consult_ad[df_consult_ad["経路"] != "Meta"] if len(df_consult_ad) > 0 else df_consult_ad
    consult_stats_sm = calc_consultation_stats(df_consult_sm)
    close_sm = calc_close_rate(df_consult_sm)

    # 全体用の目標
    kpi_all = {}
    if kpi_meta or kpi_sns:
        kpi_all["消化予算"] = kpi_meta.get("消化予算", 0)
        kpi_all["成約数"] = kpi_meta.get("成約数", 0) + kpi_sns.get("成約数", 0)
        kpi_all["成約率"] = kpi_meta.get("成約率", 0)
        kpi_all["アポ獲得数"] = kpi_meta.get("アポ獲得数", 0) + kpi_sns.get("必要アポ数", 0)
        kpi_all["アポ実施数"] = kpi_meta.get("アポ実施数", 0)
        kpi_all["アポ着席率"] = kpi_meta.get("アポ着席率", 0)
        kpi_all["リスト獲得数"] = kpi_meta.get("リスト獲得数", 0)
        kpi_all["許容CPA"] = kpi_meta.get("許容CPA", 0)

    # SNS用の目標キーを統一
    kpi_sns_unified = {}
    if kpi_sns:
        kpi_sns_unified = {
            "受注高": kpi_sns.get("受注高", 0),
            "成約数": kpi_sns.get("成約数", 0),
            "成約率": kpi_sns.get("成約率", 0),
            "アポ獲得数": kpi_sns.get("必要アポ数", 0),
            "アポ着席率": kpi_sns.get("アポ着席率", 0),
        }

    # 広告_統合（Meta + セールスマーケ）
    df_ad_combined = pd.concat([df_meta, df_sm], ignore_index=True)
    close_ad_combined = calc_close_rate(df_consult_ad)  # df_consult_adはMeta+セールスマーケ両方を含む
    consult_stats_ad_combined = calc_consultation_stats(df_consult_ad)

    # ── タブ ──
    tab_all, tab_sns, tab_ad_combined, tab_meta, tab_sm, tab_ad = st.tabs([
        f"📊 全体（{len(df_all_sales)}件）",
        f"📗 SNS（{len(df_sns)}件）",
        f"📙 広告_統合（{len(df_ad_combined)}件）",
        f"📘 Meta（{len(df_meta)}件）",
        f"🤝 セールスマーケ（{len(df_sm)}件）",
        "📈 広告ダッシュボード",
    ])

    # ====== 全体 ======
    with tab_all:
        render_summary(df_all_sales, budget=total_budget,
                       total_registrations=total_registrations,
                       close_rate_data=close_all, show_list_cpa=False,
                       targets=kpi_all)
        st.divider()
        render_consultation_stats(consult_stats_all, targets=kpi_all,
                                   actual_sales_count=len(df_all_sales))
        st.divider()
        st.subheader("成約者一覧")
        render_sales_list(df_all_sales, show_cr=True)

    # ====== 広告_統合（Meta + セールスマーケ） ======
    with tab_ad_combined:
        render_summary(df_ad_combined, budget=total_budget,
                       total_registrations=total_registrations,
                       close_rate_data=close_ad_combined, show_list_cpa=True,
                       targets=kpi_meta)
        st.divider()
        render_consultation_stats(consult_stats_ad_combined, targets=kpi_meta,
                                   actual_sales_count=len(df_ad_combined))
        st.divider()
        st.subheader("成約者一覧")
        render_sales_list(df_ad_combined, show_cr=True)

    # ====== Meta ======
    with tab_meta:
        matched_meta = df_meta[df_meta["CR番号"].notna()]
        unmatched_meta = df_meta[df_meta["CR番号"].isna()]

        render_summary(df_meta, budget=total_budget,
                       total_registrations=total_registrations,
                       close_rate_data=close_meta, show_list_cpa=True,
                       targets=kpi_meta)
        st.divider()
        render_consultation_stats(consult_stats_meta, targets=kpi_meta,
                                   actual_sales_count=len(df_meta))

        st.divider()
        detail_tab, unmatch_tab = st.tabs(["📋 成約者詳細", "⚠️ 未マッチ成約者"])
        with detail_tab:
            render_sales_list(matched_meta, show_cr=True)
        with unmatch_tab:
            st.caption("リスト流入経路にLINE名が見つからなかった成約者")
            render_sales_list(unmatched_meta)

    # ====== SNS ======
    with tab_sns:
        render_summary(df_sns, close_rate_data=close_sns, targets=kpi_sns_unified)
        st.divider()
        render_consultation_stats(consult_stats_sns, targets=kpi_sns_unified,
                                   actual_sales_count=len(df_sns))
        st.divider()
        st.subheader("成約者一覧")
        render_sales_list(df_sns)

    # ====== セールスマーケ ======
    with tab_sm:
        render_summary(df_sm, close_rate_data=close_sm)
        st.divider()
        render_consultation_stats(consult_stats_sm,
                                   actual_sales_count=len(df_sm))
        st.divider()
        st.subheader("成約者一覧")
        render_sales_list(df_sm)

    # ====== 広告ダッシュボード ======
    with tab_ad:
        def _ad_dashboard():
            from datetime import timedelta
            dates = sorted(df_budget_raw["Day"].unique())
            if not dates:
                st.info("広告データなし")
                return

            # ── ヘッダー: 期間プリセット選択 ──
            today = pd.Timestamp.now().normalize()
            presets = ["今月", "今日", "昨日", "過去7日間", "過去30日間", "過去90日間", "カスタム期間"]
            col_preset, col_custom1, col_custom2, col_spacer = st.columns([1.2, 0.8, 0.8, 1.2])
            with col_preset:
                selected_preset = st.selectbox("期間", presets, index=0, key="ad_period_preset", label_visibility="collapsed")

            if selected_preset == "今月":
                d_start = today.replace(day=1)
                d_end = today
            elif selected_preset == "今日":
                d_start, d_end = today, today
            elif selected_preset == "昨日":
                d_start = today - timedelta(days=1)
                d_end = d_start
            elif selected_preset == "過去7日間":
                d_start = today - timedelta(days=6)
                d_end = today
            elif selected_preset == "過去30日間":
                d_start = today - timedelta(days=29)
                d_end = today
            elif selected_preset == "過去90日間":
                d_start = today - timedelta(days=89)
                d_end = today
            else:
                with col_custom1:
                    d_start = st.date_input("開始日", value=pd.to_datetime(dates[0]),
                                            min_value=pd.to_datetime(dates[0]),
                                            max_value=pd.to_datetime(dates[-1]),
                                            key="ad_custom_start")
                with col_custom2:
                    d_end = st.date_input("終了日", value=pd.to_datetime(dates[-1]),
                                          min_value=pd.to_datetime(dates[0]),
                                          max_value=pd.to_datetime(dates[-1]),
                                          key="ad_custom_end")

            d_start_str = str(pd.Timestamp(d_start).date())
            d_end_str = str(pd.Timestamp(d_end).date())
            df_filtered = df_budget_raw[
                (df_budget_raw["Day"] >= d_start_str) &
                (df_budget_raw["Day"] <= d_end_str)
            ]
            regs_filtered = regs_by_day_cr[
                (regs_by_day_cr["登録日"] >= d_start_str) &
                (regs_by_day_cr["登録日"] <= d_end_str)
            ]
            period_label = f"{d_start_str} - {d_end_str}"

            # ── サマリーカード ──
            total_spend = int(df_filtered["金額"].sum())
            num_campaigns = df_filtered["Campaign Name"].nunique()
            total_regs_period = int(regs_filtered["登録数"].sum()) if len(regs_filtered) > 0 else 0
            avg_cpa = int(total_spend / total_regs_period) if total_regs_period > 0 else 0

            st.markdown(f"**{period_label}**")
            row = st.columns(4)
            row[0].metric("消化金額", format_yen(total_spend))
            row[1].metric("キャンペーン数", f"{num_campaigns}")
            row[2].metric("コンバージョン数（LINE登録）", f"{total_regs_period}")
            row[3].metric("CPA", format_yen(avg_cpa))

            st.divider()

            # キャンペーン名単位の集計（集計テーブル用）
            camp_budget = df_filtered.groupby("Campaign Name")["金額"].sum().reset_index()
            camp_budget.columns = ["キャンペーン名", "消化予算"]

            cr_to_camp = df_filtered[["Campaign Name", "CR詳細"]].drop_duplicates()
            camp_regs_raw = regs_filtered.merge(cr_to_camp, on="CR詳細", how="inner")
            camp_regs_raw = camp_regs_raw.groupby("Campaign Name")["登録数"].sum().reset_index()
            camp_regs_raw.columns = ["キャンペーン名", "LINE登録数"]

            # CR詳細を共有するキャンペーンがある場合、予算比率で按分
            cr_camp_count = cr_to_camp.groupby("CR詳細")["Campaign Name"].count()
            shared_crs = set(cr_camp_count[cr_camp_count > 1].index)
            if shared_crs:
                camp_regs_list = []
                for _, row in camp_regs_raw.iterrows():
                    cname = row["キャンペーン名"]
                    cr_detail = cr_to_camp[cr_to_camp["Campaign Name"] == cname]["CR詳細"].iloc[0] if len(cr_to_camp[cr_to_camp["Campaign Name"] == cname]) > 0 else None
                    if cr_detail in shared_crs:
                        # このCR詳細を共有する全キャンペーンの予算を取得
                        sibling_camps = cr_to_camp[cr_to_camp["CR詳細"] == cr_detail]["Campaign Name"].tolist()
                        sibling_budgets = camp_budget[camp_budget["キャンペーン名"].isin(sibling_camps)]
                        total_budget = sibling_budgets["消化予算"].sum()
                        my_budget = camp_budget[camp_budget["キャンペーン名"] == cname]["消化予算"].sum()
                        ratio = my_budget / total_budget if total_budget > 0 else 1 / len(sibling_camps)
                        camp_regs_list.append({"キャンペーン名": cname, "LINE登録数": int(round(row["LINE登録数"] * ratio))})
                    else:
                        camp_regs_list.append({"キャンペーン名": cname, "LINE登録数": int(row["LINE登録数"])})
                camp_regs = pd.DataFrame(camp_regs_list)
            else:
                camp_regs = camp_regs_raw

            df_camp_summary = camp_budget.merge(camp_regs, on="キャンペーン名", how="outer").fillna(0)
            df_camp_summary["消化予算"] = df_camp_summary["消化予算"].astype(int)
            df_camp_summary["LINE登録数"] = df_camp_summary["LINE登録数"].astype(int)
            df_camp_summary["CPA"] = df_camp_summary.apply(
                lambda r: int(r["消化予算"] / r["LINE登録数"]) if r["LINE登録数"] > 0 else 0, axis=1
            )

            # ── キャンペーン別パフォーマンス（集計） ──
            st.subheader("キャンペーン別パフォーマンス（集計）")
            display_camp = df_camp_summary.sort_values("消化予算", ascending=False).copy()
            display_camp["消化予算"] = display_camp["消化予算"].apply(lambda x: f"¥{x:,.0f}")
            display_camp["CPA"] = display_camp["CPA"].apply(lambda x: f"¥{x:,.0f}" if x > 0 else "-")
            st.dataframe(
                display_camp,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "キャンペーン名": st.column_config.TextColumn("キャンペーン名", width="large"),
                    "消化予算": st.column_config.TextColumn("消化予算", width="medium"),
                    "LINE登録数": st.column_config.NumberColumn("LINE登録数", width="small"),
                    "CPA": st.column_config.TextColumn("CPA", width="medium"),
                },
            )

            st.divider()

            # ── キャンペーン×日付 ピボットテーブル ──
            st.subheader("キャンペーン別 日付×指標")

            # ピボット専用の日付フィルタ
            pv_presets = ["ダッシュボードと同じ", "今月", "今日", "昨日", "過去7日間", "過去14日間", "過去30日間", "カスタム期間"]
            pv_col1, pv_col2, pv_col3 = st.columns([1.2, 0.8, 0.8])
            with pv_col1:
                pv_preset = st.selectbox("期間", pv_presets, index=0, key="pv_period", label_visibility="collapsed")

            if pv_preset == "ダッシュボードと同じ":
                pv_start_str, pv_end_str = d_start_str, d_end_str
            elif pv_preset == "今月":
                pv_start_str = str(today.replace(day=1).date())
                pv_end_str = str(today.date())
            elif pv_preset == "今日":
                pv_start_str = pv_end_str = str(today.date())
            elif pv_preset == "昨日":
                yesterday = today - timedelta(days=1)
                pv_start_str = pv_end_str = str(yesterday.date())
            elif pv_preset == "過去7日間":
                pv_start_str = str((today - timedelta(days=6)).date())
                pv_end_str = str(today.date())
            elif pv_preset == "過去14日間":
                pv_start_str = str((today - timedelta(days=13)).date())
                pv_end_str = str(today.date())
            elif pv_preset == "過去30日間":
                pv_start_str = str((today - timedelta(days=29)).date())
                pv_end_str = str(today.date())
            else:
                with pv_col2:
                    pv_start = st.date_input("開始", value=pd.to_datetime(dates[0]),
                                             min_value=pd.to_datetime(dates[0]),
                                             max_value=pd.to_datetime(dates[-1]),
                                             key="pv_start")
                with pv_col3:
                    pv_end = st.date_input("終了", value=pd.to_datetime(dates[-1]),
                                           min_value=pd.to_datetime(dates[0]),
                                           max_value=pd.to_datetime(dates[-1]),
                                           key="pv_end")
                pv_start_str = str(pd.Timestamp(pv_start).date())
                pv_end_str = str(pd.Timestamp(pv_end).date())

            pv_budget = df_budget_raw[
                (df_budget_raw["Day"] >= pv_start_str) & (df_budget_raw["Day"] <= pv_end_str)
            ]
            pv_regs = regs_by_day_cr[
                (regs_by_day_cr["登録日"] >= pv_start_str) & (regs_by_day_cr["登録日"] <= pv_end_str)
            ]

            # 期間内の日付リスト
            filtered_dates = sorted(pv_budget["Day"].unique())

            # 広告費: CR詳細 × Day
            budget_pivot = pv_budget.groupby(["CR詳細", "Day"])["金額"].sum()
            budget_by_detail = pv_budget.groupby("CR詳細")["金額"].sum()

            # 獲得リスト数: CR詳細 × 登録日
            regs_pivot = pv_regs.groupby(["CR詳細", "登録日"])["登録数"].sum()
            regs_by_detail = pv_regs.groupby("CR詳細")["登録数"].sum()

            # アポ獲得数: CR詳細 × 日付（審査落ち・日程調整中を除外）
            apo_by_detail_day = pd.Series(dtype=int)
            apo_by_detail_total = pd.Series(dtype=int)
            if len(df_consult_ad) > 0 and "CR詳細" in df_consult_ad.columns and "日付" in df_consult_ad.columns:
                df_ca = df_consult_ad[
                    (df_consult_ad["CR詳細"].notna()) &
                    (df_consult_ad["日付"] >= pv_start_str) &
                    (df_consult_ad["日付"] <= pv_end_str) &
                    (~df_consult_ad["ステータス"].str.contains("審査落ち|日程調整", na=False))
                ]
                if len(df_ca) > 0:
                    apo_by_detail_day = df_ca.groupby(["CR詳細", "日付"]).size()
                    apo_by_detail_total = df_ca.groupby("CR詳細").size()

            # 売上: CR詳細（合計のみ、日付別は不可）
            sales_by_detail = pd.Series(dtype=int)
            if len(df_all_sales) > 0 and "CR詳細" in df_all_sales.columns:
                matched = df_all_sales[df_all_sales["CR詳細"].notna()]
                if len(matched) > 0:
                    sales_by_detail = matched.groupby("CR詳細")["受注金額"].sum()

            # 全CR詳細を収集（消化予算降順）
            all_cr_set = (
                set(budget_by_detail.index)
                | set(regs_by_detail.index)
                | (set(apo_by_detail_total.index) if len(apo_by_detail_total) > 0 else set())
                | (set(sales_by_detail.index) if len(sales_by_detail) > 0 else set())
            )
            all_cr_details = sorted(
                all_cr_set,
                key=lambda x: budget_by_detail.get(x, 0),
                reverse=True,
            )

            if all_cr_details:
                metrics = ["広告費", "獲得リスト数", "CPA", "アポ獲得数", "売上", "ROAS"]
                num_metrics = len(metrics)
                # キャンペーン交互色
                camp_colors = ["#ffffff", "#f7f9fc"]

                # HTMLテーブル構築
                html_parts = []
                html_parts.append('<div class="pivot-wrap"><table class="pivot-tbl">')
                # ヘッダー
                html_parts.append('<thead><tr>')
                html_parts.append('<th class="sticky-col sc0">キャンペーン</th>')
                html_parts.append('<th class="sticky-col sc1">指標</th>')
                html_parts.append('<th class="sticky-col sc2">合計</th>')
                for d in filtered_dates:
                    day_num = d[8:].lstrip("0") or "0"  # "2026-03-01" → "1"
                    month_num = d[5:7].lstrip("0")
                    html_parts.append(f'<th>{month_num}/{day_num}</th>')
                html_parts.append('</tr></thead><tbody>')

                for ci, cr in enumerate(all_cr_details):
                    bg = camp_colors[ci % 2]
                    b_total = int(budget_by_detail.get(cr, 0))
                    r_total = int(regs_by_detail.get(cr, 0))
                    a_total = int(apo_by_detail_total.get(cr, 0)) if len(apo_by_detail_total) > 0 else 0
                    s_total = int(sales_by_detail.get(cr, 0)) if len(sales_by_detail) > 0 else 0

                    for mi, metric in enumerate(metrics):
                        html_parts.append(f'<tr style="background:{bg}">')
                        # キャンペーン名セル（最初の指標行のみrowspan）
                        if mi == 0:
                            html_parts.append(f'<td class="sticky-col sc0 camp-cell" rowspan="{num_metrics}" style="background:{bg}">{cr}</td>')
                        # 指標名
                        html_parts.append(f'<td class="sticky-col sc1" style="background:{bg}">{metric}</td>')

                        # 合計値
                        if metric == "広告費":
                            total_val = f"¥{b_total:,}" if b_total else "-"
                        elif metric == "獲得リスト数":
                            total_val = str(r_total) if r_total else "-"
                        elif metric == "CPA":
                            total_val = f"¥{b_total // r_total:,}" if r_total > 0 else "-"
                        elif metric == "アポ獲得数":
                            total_val = str(a_total) if a_total else "-"
                        elif metric == "売上":
                            total_val = f"¥{s_total:,}" if s_total else "-"
                        elif metric == "ROAS":
                            total_val = f"{round(s_total / b_total * 100, 1)}%" if b_total > 0 else "-"
                        html_parts.append(f'<td class="sticky-col sc2 total-cell" style="background:{bg}">{total_val}</td>')

                        # 日別値
                        for d in filtered_dates:
                            b = int(budget_pivot.get((cr, d), 0))
                            r = int(regs_pivot.get((cr, d), 0))
                            a = int(apo_by_detail_day.get((cr, d), 0)) if len(apo_by_detail_day) > 0 else 0

                            if metric == "広告費":
                                v = f"¥{b:,}" if b else "-"
                            elif metric == "獲得リスト数":
                                v = str(r) if r else "-"
                            elif metric == "CPA":
                                v = f"¥{b // r:,}" if r > 0 else "-"
                            elif metric == "アポ獲得数":
                                v = str(a) if a else "-"
                            elif metric == "売上":
                                v = "-"
                            elif metric == "ROAS":
                                v = "-"
                            html_parts.append(f'<td>{v}</td>')
                        html_parts.append('</tr>')

                html_parts.append('</tbody></table></div>')

                # CSS
                pivot_css = """
<style>
.pivot-wrap { overflow-x: auto; max-height: 800px; overflow-y: auto; border: 1px solid #ddd; border-radius: 8px; }
.pivot-tbl { border-collapse: separate; border-spacing: 0; font-size: 12px; white-space: nowrap; }
.pivot-tbl th, .pivot-tbl td { padding: 4px 10px; border-bottom: 1px solid #e8e8e8; border-right: 1px solid #f0f0f0; text-align: right; }
.pivot-tbl th { background: #f5f5f5; position: sticky; top: 0; z-index: 3; font-weight: 600; }
.pivot-tbl .camp-cell { font-weight: 700; text-align: left; vertical-align: middle; border-right: 2px solid #ddd; }
.pivot-tbl .total-cell { font-weight: 600; border-right: 2px solid #ccc; }
.sticky-col { position: sticky; z-index: 2; }
.sc0 { left: 0; min-width: 110px; text-align: left; }
.sc1 { left: 110px; min-width: 90px; text-align: left; border-right: 1px solid #ddd; }
.sc2 { left: 200px; min-width: 90px; border-right: 2px solid #ccc; }
.pivot-tbl thead .sticky-col { z-index: 4; }
.pivot-tbl tr:hover td { background: #fffde7 !important; }
</style>
"""
                st.markdown(pivot_css + "".join(html_parts), unsafe_allow_html=True)
            else:
                st.info("データなし")

            # ── 広告費 vs 売上（キャンペーン別）── ピボットテーブルと同じ日付範囲
            st.divider()
            st.subheader("広告費 vs 売上（キャンペーン別）")
            st.caption(f"期間: {pv_start_str} 〜 {pv_end_str}")

            # チャート期間の予算集計（1円でも消化したキャンペーンのみ）- ピボットと同じ日付範囲
            ch_camp_budget = pv_budget.groupby("Campaign Name")["金額"].sum().reset_index()
            ch_camp_budget.columns = ["キャンペーン名", "消化予算"]
            ch_camp_budget = ch_camp_budget[ch_camp_budget["消化予算"] > 0]

            # CR詳細→キャンペーン名マッピングで正確に売上を紐づけ
            matched_meta_ad = df_meta[df_meta["CR詳細"].notna()].copy()
            detail_to_camp = pv_budget[["Campaign Name", "CR詳細"]].drop_duplicates()
            matched_meta_ad = matched_meta_ad.merge(detail_to_camp, on="CR詳細", how="left")
            camp_sales = matched_meta_ad[matched_meta_ad["Campaign Name"].notna()].groupby(
                "Campaign Name")["受注金額"].sum().reset_index()
            camp_sales.columns = ["キャンペーン名", "売上合計"]

            chart_data = ch_camp_budget.merge(
                camp_sales, on="キャンペーン名", how="left"
            ).fillna(0)
            chart_data["売上合計"] = chart_data["売上合計"].astype(int)

            if len(chart_data) > 0:
                chart_data["短縮名"] = chart_data["キャンペーン名"].apply(short_camp_name)
                camp_order = chart_data.sort_values("消化予算", ascending=False)["短縮名"].tolist()
                melted = chart_data[["短縮名", "消化予算", "売上合計"]].melt(
                    id_vars="短縮名", value_vars=["消化予算", "売上合計"],
                    var_name="項目", value_name="金額"
                )
                melted["項目"] = melted["項目"].map({"消化予算": "広告費", "売上合計": "売上"})

                chart = (
                    alt.Chart(melted)
                    .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                    .encode(
                        x=alt.X("短縮名:N", sort=camp_order, title=None,
                                 axis=alt.Axis(labelAngle=-30, labelFontSize=11, labelLimit=200)),
                        y=alt.Y("金額:Q", title=None,
                                 axis=alt.Axis(format="~s", labelFontSize=11)),
                        xOffset=alt.XOffset("項目:N"),
                        color=alt.Color(
                            "項目:N",
                            scale=alt.Scale(domain=["売上", "広告費"], range=["#4CAF50", "#E53935"]),
                            legend=alt.Legend(title=None, orient="bottom", labelFontSize=13),
                        ),
                        tooltip=[
                            alt.Tooltip("短縮名:N", title="キャンペーン"),
                            alt.Tooltip("項目:N"),
                            alt.Tooltip("金額:Q", format=",.0f"),
                        ],
                    )
                    .properties(height=400)
                    .configure_view(strokeWidth=0)
                )
                st.altair_chart(chart, use_container_width=True)

        _ad_dashboard()

    # リフレッシュ
    st.divider()
    if st.button("🔄 データを再読み込み"):
        st.cache_data.clear()
        st.rerun()


if __name__ == "__main__":
    main()
