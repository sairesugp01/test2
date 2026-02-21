# app_v3_fixed.py の先頭(シンプル版)
import streamlit as st
import pandas as pd
from bs4 import BeautifulSoup
import unicodedata
import re
from datetime import datetime
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, KeepTogether
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.pdfgen import canvas as rl_canvas
from reportlab.platypus import BaseDocTemplate, Frame, PageTemplate

# インポートのデバッグ情報
import sys
import os

# sys.pathに現在のディレクトリを追加(デバッグ表示なし)
try:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
except Exception as e:
    pass


# set_page_config は必ず最初のStreamlitコマンドにする
st.set_page_config(page_title="競馬予想AI v9.0 (Scorer v7)", page_icon="🏇", layout="wide")

# scraperのインポート
_import_error = None
try:
    from scraper_v5 import NetkeibaRaceScraper
except ImportError as e:
    _import_error = str(e)

if _import_error:
    st.error(f"❌ **Import Error**: {_import_error}")
    st.error("""
    **解決方法**:
    1. `scraper_v5.py` と `enhanced_scorer_v7.py` が同じディレクトリにあることを確認
    2. Streamlit Cloudの場合、GitHubリポジトリのルートに全ファイルを配置
    3. ファイル名が正確に一致しているか確認(大文字小文字も含む)
    """)
    st.stop()

# --- 日本語フォント設定 ---
@st.cache_resource
def setup_japanese_font():
    """日本語フォントの設定"""
    try:
        # CIDフォント(ReportLab組み込み)を使用
        from reportlab.pdfbase.cidfonts import UnicodeCIDFont
        pdfmetrics.registerFont(UnicodeCIDFont('HeiseiMin-W3'))
        return 'HeiseiMin-W3'
    except Exception:
        return 'Helvetica'

try:
    JAPANESE_FONT = setup_japanese_font()
except Exception:
    JAPANESE_FONT = 'Helvetica' 

# --- 共通ユーティリティ ---
def normalize_uma(s):
    """馬番の正規化"""
    if s is None: 
        return ""
    return unicodedata.normalize('NFKC', str(s)).strip().lstrip('0')

def fetch_results_simple(race_id):
    """レース結果を取得（着順・人気・オッズ）"""
    results = {}
    try:
        scraper = NetkeibaRaceScraper()
        res = scraper.session.get(f"https://race.netkeiba.com/race/result.html?race_id={race_id}", timeout=10)
        res.encoding = 'EUC-JP'
        soup = BeautifulSoup(res.content, "html.parser")
        table = soup.find("table", id="All_Result_Table")
        if table:
            for row in table.find_all("tr")[1:]:
                tds = row.find_all("td")
                if len(tds) >= 3:
                    rank  = tds[0].get_text(strip=True)
                    u_no  = normalize_uma(tds[2].get_text(strip=True))
                    # 人気・オッズは列9・10（存在する場合のみ）
                    pop   = tds[9].get_text(strip=True)  if len(tds) > 9  else "-"
                    odds  = tds[10].get_text(strip=True) if len(tds) > 10 else "-"
                    if u_no:
                        results[u_no] = {
                            "rank": rank,
                            "pop":  pop,
                            "odds": odds,
                        }
    except Exception as e:
        st.warning(f"結果取得エラー: {e}")
    return results

def prepare_display_df(raw_df, results):
    """表示用データフレームを準備(着順ソート対応)"""
    # 空チェック
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()
    
    # 防御的修正:列名を'指数'に統一
    if '総合指数' in raw_df.columns:
        raw_df = raw_df.rename(columns={'総合指数': '指数'})
    
    if '指数' not in raw_df.columns:
        raw_df['指数'] = 0.0
    
    data = []
    for _, r in raw_df.iterrows():
        u_no_str = normalize_uma(r.get('馬番',''))
        res_data = results.get(u_no_str, {})

        # 辞書形式(新)と文字列形式(旧)の両方に対応
        if isinstance(res_data, dict):
            rank_str = res_data.get("rank", "-")
            pop_str  = res_data.get("pop",  "-")
            odds_str = res_data.get("odds", "-")
        else:
            rank_str = res_data if res_data else "-"
            pop_str  = "-"
            odds_str = "-"

        # 着順・馬番を数値に変換(ソートを正常化)
        try:
            u_no_val = int(u_no_str) if u_no_str.isdigit() else 99
        except Exception:
            u_no_val = 99

        try:
            # 「1」は1に、「中止」は999にする
            rank_val = int(re.sub(r'\D', '', rank_str)) if re.search(r'\d', rank_str) else 999
        except Exception:
            rank_val = 999

        # 人気を数値化(ソート・表示用)
        try:
            pop_val = int(re.sub(r'\D', '', pop_str)) if re.search(r'\d', pop_str) else 99
        except Exception:
            pop_val = 99

        data.append({
            "印":   r.get('印',''),
            "馬番": u_no_val,
            "馬名": r.get('馬名',''),
            "指数": float(r.get('指数', 0)),
            "人気": pop_val  if pop_str  != "-" else "-",
            "オッズ": odds_str if odds_str != "-" else "-",
            "着順": rank_val if rank_str != "-" else "-",
            "_sort_rank": rank_val,
            "_pop_val":   pop_val,
        })
    
    df = pd.DataFrame(data)
    
    # 結果照合時(着順データがある場合)は着順でソート
    if not df.empty and results:
        df = df.sort_values('_sort_rank').reset_index(drop=True)
        df = df.drop(columns=['_sort_rank', '_pop_val'], errors='ignore')
    else:
        # 分析時は指数の高い順でソート(印と一致させる)
        df = df.sort_values('指数', ascending=False).reset_index(drop=True)
        df = df.drop(columns=['_sort_rank', '_pop_val'], errors='ignore')
        # 分析モードでは人気・オッズ列を削除（データなし）
        df = df.drop(columns=['人気', 'オッズ'], errors='ignore')

    # 表示用の着順列を「数値」として扱うことで 1.2.10 の順になる
    if not df.empty:
        df["着順"] = pd.to_numeric(df["着順"], errors='coerce')
        if "人気" in df.columns:
            df["人気"] = pd.to_numeric(df["人気"], errors='coerce').astype("Int64")
    
    return df

# =====================================================================
# カラーパレット定義
# =====================================================================
PDF_DARK    = colors.HexColor('#0D1117')   # 最暗背景
PDF_NAVY    = colors.HexColor('#1A2340')   # ヘッダー背景
PDF_GOLD    = colors.HexColor('#C9A84C')   # アクセント金
PDF_GOLD2   = colors.HexColor('#F0D080')   # 薄い金
PDF_RED     = colors.HexColor('#C0392B')   # 1着ハイライト
PDF_SILVER  = colors.HexColor('#7F8C8D')   # 2着
PDF_BRONZE  = colors.HexColor('#A04000')   # 3着
PDF_ROW_A   = colors.HexColor('#F7F9FC')   # 偶数行
PDF_ROW_B   = colors.HexColor('#FFFFFF')   # 奇数行
PDF_BORDER  = colors.HexColor('#C8D0DC')   # 罫線
PDF_TEXT    = colors.HexColor('#1A1A2E')   # 本文テキスト
PDF_HEAD_TXT= colors.HexColor('#FFFFFF')   # ヘッダーテキスト
PDF_ACCENT  = colors.HexColor('#2C3E7A')   # サブヘッダー

# 印→色マッピング
MARK_COLORS = {
    '◎': colors.HexColor('#C0392B'),
    '○': colors.HexColor('#2471A3'),
    '▲': colors.HexColor('#1E8449'),
    '△': colors.HexColor('#7D3C98'),
    '×': colors.HexColor('#717D7E'),
}

def _draw_page_background(c, doc):
    """各ページにヘッダーバーと装飾を描画"""
    W, H = A4
    # 上部ゴールドライン
    c.setFillColor(PDF_GOLD)
    c.rect(0, H - 4*mm, W, 4*mm, fill=1, stroke=0)
    # 下部ゴールドライン
    c.rect(0, 0, W, 2*mm, fill=1, stroke=0)
    # フッターテキスト
    c.setFillColor(PDF_SILVER)
    c.setFont(JAPANESE_FONT, 7)
    c.drawCentredString(W/2, 4*mm, f"競馬予想AI  -  {doc._report_venue}  {doc._report_date}")


# =====================================================================
# カスタムFlowable: 指数バーグラフ付きセル
# =====================================================================
from reportlab.platypus.flowables import Flowable

class ScoreBarCell(Flowable):
    """指数バーグラフ: 濃色バー＋右端に数値ラベル付き"""
    def __init__(self, score, max_score, bar_width, row_height, rank):
        super().__init__()
        self.score      = score
        self.max_score  = max_score if max_score > 0 else 1
        self.bar_width  = bar_width
        self.row_height = row_height
        self.rank       = rank      # 1始まりの順位（色グラデーション用）
        self.width      = bar_width
        self.height     = row_height

    def draw(self):
        c   = self.canv
        w   = self.bar_width
        h   = self.row_height
        ratio = min(self.score / self.max_score, 1.0)

        PAD_L  = 3      # 左余白
        PAD_R  = 26     # 右余白（数値ラベル用）
        bar_h  = 5.5    # バーの高さ（太め）
        bar_y  = (h - bar_h) / 2
        avail  = w - PAD_L - PAD_R

        # ---- バー背景（ネイビー系の暗めグレー） ----
        c.setFillColor(colors.HexColor('#CBD5E0'))
        c.roundRect(PAD_L, bar_y, avail, bar_h, 2, fill=1, stroke=0)

        # ---- バー本体（上位ほど鮮やかなゴールド→ネイビーグラデーション） ----
        if ratio > 0.01:
            # 順位に応じてゴールド(1位)→ネイビー(下位)へ色変化
            t = min((self.rank - 1) / 9, 1.0)   # 0.0(1位)〜1.0(10位以下)
            # ゴールド #C9A84C → ネイビー #2C3E7A
            r = int(0xC9 + t * (0x2C - 0xC9))
            g = int(0xA8 + t * (0x3E - 0xA8))
            b = int(0x4C + t * (0x7A - 0x4C))
            bar_color = colors.Color(r/255, g/255, b/255)

            bar_w = max(avail * ratio, 4)
            c.setFillColor(bar_color)
            c.roundRect(PAD_L, bar_y, bar_w, bar_h, 2, fill=1, stroke=0)

            # バー上に細いハイライトライン（立体感）
            c.setFillColor(colors.Color(1, 1, 1, 0.3))
            c.roundRect(PAD_L + 1, bar_y + bar_h - 1.5, bar_w - 2, 1.2, 0.5, fill=1, stroke=0)

        # ---- 右端に数値ラベル ----
        label = f"{self.score:.1f}"
        c.setFont('HeiseiMin-W3', 7.5)
        c.setFillColor(colors.HexColor('#1A2340'))
        c.drawRightString(w - 2, bar_y - 0.5, label)


def _make_race_table(df, font, mode):
    """レース1本分のテーブルを生成"""
    has_odds = '人気' in df.columns and '着順' in df.columns
    # 予想モード: 着順なし・グラフあり
    # 結果モード: 着順あり・人気・オッズ・グラフなし
    is_result = has_odds

    ROW_H = 7*mm   # 行の高さ

    if is_result:
        headers = ['印', '馬番', '馬名', '指数', '人気', 'オッズ', '着順']
        col_w   = [11*mm, 12*mm, 52*mm, 18*mm, 13*mm, 18*mm, 13*mm]
    else:
        headers = ['印', '馬番', '馬名', '指数', '指数グラフ']
        col_w   = [11*mm, 12*mm, 52*mm, 18*mm, 55*mm]

    # 最大指数（グラフ正規化用）
    try:
        max_score = float(df['指数'].max())
    except Exception:
        max_score = 100.0

    table_data = [headers]
    rank_rows  = {}

    for i, (_, row) in enumerate(df.iterrows(), start=1):
        mark   = str(row.get('印', ''))
        uma_no = str(row.get('馬番', ''))
        name   = str(row.get('馬名', ''))[:16]
        try:
            score_f = float(row.get('指数', 0))
        except Exception:
            score_f = 0.0
        score_s = f"{score_f:.1f}"

        if is_result:
            pop_v  = row.get('人気')
            rnk_v  = row.get('着順')
            pop_s  = str(int(pop_v)) if pd.notna(pop_v) else '-'
            rnk_s  = str(int(rnk_v)) if pd.notna(rnk_v) else '-'
            odds_s = str(row.get('オッズ', '-'))
            try:
                rank_rows[i] = int(rnk_v) if pd.notna(rnk_v) else 99
            except Exception:
                rank_rows[i] = 99
            table_data.append([mark, uma_no, name, score_s, pop_s, odds_s, rnk_s])
        else:
            bar_cell = ScoreBarCell(score_f, max_score, col_w[-1], ROW_H, rank=i)
            table_data.append([mark, uma_no, name, score_s, bar_cell])

    tbl = Table(table_data, colWidths=col_w, repeatRows=1, rowHeights=[ROW_H] * len(table_data))

    # ---- ベーススタイル ----
    style_cmds = [
        # ヘッダー行：白文字
        ('BACKGROUND',    (0, 0), (-1, 0),  PDF_ACCENT),
        ('TEXTCOLOR',     (0, 0), (-1, 0),  colors.white),
        ('FONTNAME',      (0, 0), (-1, 0),  font),
        ('FONTSIZE',      (0, 0), (-1, 0),  8.5),
        ('TOPPADDING',    (0, 0), (-1, 0),  4),
        ('BOTTOMPADDING', (0, 0), (-1, 0),  4),
        ('ALIGN',         (0, 0), (-1, 0),  'CENTER'),
        # データ行共通
        ('FONTNAME',      (0, 1), (-1, -1), font),
        ('FONTSIZE',      (0, 1), (-1, -1), 8.5),
        ('ALIGN',         (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN',        (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING',    (0, 1), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 0),
        # 馬名：中央寄せ・太字フォント
        ('ALIGN',         (2, 1), (2, -1),  'CENTER'),
        ('FONTNAME',      (2, 1), (2, -1),  font),
        ('FONTSIZE',      (2, 1), (2, -1),  9),
        # 罫線
        ('LINEBELOW',     (0, 0), (-1, 0),  1.2, PDF_GOLD),
        ('LINEBELOW',     (0, 1), (-1, -1), 0.3, PDF_BORDER),
        ('BOX',           (0, 0), (-1, -1), 0.8, PDF_ACCENT),
        # ゼブラ
        ('ROWBACKGROUNDS',(0, 1), (-1, -1), [PDF_ROW_B, PDF_ROW_A]),
        ('TEXTCOLOR',     (0, 1), (-1, -1), PDF_TEXT),
        # グラフ列はパディングゼロ
        ('LEFTPADDING',   (4, 1), (4, -1),  0),
        ('RIGHTPADDING',  (4, 1), (4, -1),  0),
        ('TOPPADDING',    (4, 1), (4, -1),  0),
        ('BOTTOMPADDING', (4, 1), (4, -1),  0),
    ]

    # ---- 印の色付け（◎○▲△×） ----
    MARK_TC = {
        '◎': colors.HexColor('#C0392B'),
        '○': colors.HexColor('#1A5276'),
        '▲': colors.HexColor('#1E8449'),
        '△': colors.HexColor('#6C3483'),
        '×': colors.HexColor('#717D7E'),
    }
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        mk = str(row.get('印', ''))
        if mk in MARK_TC:
            style_cmds.append(('TEXTCOLOR', (0, i), (0, i), MARK_TC[mk]))
            style_cmds.append(('FONTSIZE',  (0, i), (0, i), 10))

    # ---- 着順ハイライト（結果モードのみ） ----
    if is_result:
        for row_i, rnk in rank_rows.items():
            if rnk == 1:
                bg, tc = colors.HexColor('#FFF3CD'), PDF_RED
            elif rnk == 2:
                bg, tc = colors.HexColor('#EAF4FB'), colors.HexColor('#1A5276')
            elif rnk == 3:
                bg, tc = colors.HexColor('#FDF3E3'), PDF_BRONZE
            else:
                continue
            last_col = len(headers) - 1
            style_cmds += [
                ('BACKGROUND', (0, row_i), (-1, row_i), bg),
                ('TEXTCOLOR',  (last_col, row_i), (last_col, row_i), tc),
                ('FONTSIZE',   (last_col, row_i), (last_col, row_i), 10),
            ]

    tbl.setStyle(TableStyle(style_cmds))
    return tbl


def create_pdf_report(batch_data, venue, date, mode="analysis"):
    """PDF予想レポートを生成（プレミアムデザイン版）"""
    buffer   = BytesIO()
    W, H     = A4
    L_MARGIN = 14*mm
    R_MARGIN = 14*mm
    T_MARGIN = 18*mm
    B_MARGIN = 14*mm

    report_type   = "予想レポート" if mode == "analysis" else "結果照合レポート"
    date_formatted = f"{date[:4]}年{date[4:6]}月{date[6:8]}日"
    FN = JAPANESE_FONT

    # --- スタイル定義 ---
    styles = getSampleStyleSheet()

    race_heading_style = ParagraphStyle(
        'RaceHeading', parent=styles['Normal'],
        fontName=FN, fontSize=11, textColor=PDF_HEAD_TXT,
        spaceAfter=0, spaceBefore=0, leading=14,
    )
    condition_style = ParagraphStyle(
        'Condition', parent=styles['Normal'],
        fontName=FN, fontSize=8, textColor=PDF_GOLD2,
        spaceAfter=0, spaceBefore=0,
    )
    nodata_style = ParagraphStyle(
        'NoData', parent=styles['Normal'],
        fontName=FN, fontSize=9, textColor=PDF_SILVER,
        alignment=TA_CENTER,
    )

    # --- ページヘッダーを描画するクラス ---
    class RacingDocTemplate(BaseDocTemplate):
        def __init__(self, *args, **kwargs):
            self._report_venue = kwargs.pop('report_venue', '')
            self._report_date  = kwargs.pop('report_date', '')
            self._report_type  = kwargs.pop('report_type', '')
            super().__init__(*args, **kwargs)

        def handle_pageBegin(self):
            super().handle_pageBegin()
            self._draw_header()

        def _draw_header(self):
            c = self.canv
            # 上部ゴールドバー
            c.setFillColor(PDF_GOLD)
            c.rect(0, H - 4*mm, W, 4*mm, fill=1, stroke=0)
            # ネイビーヘッダーバー
            c.setFillColor(PDF_NAVY)
            c.rect(0, H - 18*mm, W, 14*mm, fill=1, stroke=0)
            # 左側：ゴールドの縦ラインアクセント
            c.setFillColor(PDF_GOLD)
            c.rect(L_MARGIN, H - 16.5*mm, 3, 10, fill=1, stroke=0)
            # タイトル文字（絵文字なし・CIDフォント安全）
            c.setFillColor(PDF_GOLD)
            c.setFont(FN, 13)
            c.drawString(L_MARGIN + 6*mm, H - 14*mm,
                         f"{self._report_venue}  {self._report_type}")
            # 右側に日付
            c.setFillColor(PDF_GOLD2)
            c.setFont(FN, 9)
            c.drawRightString(W - R_MARGIN, H - 14*mm, self._report_date)
            # 下部ゴールドライン（ヘッダー下）
            c.setFillColor(PDF_GOLD)
            c.rect(0, H - 19*mm, W, 1.2*mm, fill=1, stroke=0)
            # フッター背景
            c.setFillColor(PDF_NAVY)
            c.rect(0, 0, W, 7*mm, fill=1, stroke=0)
            c.setFillColor(PDF_GOLD)
            c.rect(0, 7*mm, W, 0.8*mm, fill=1, stroke=0)
            # フッターテキスト
            c.setFillColor(PDF_GOLD2)
            c.setFont(FN, 6.5)
            c.drawCentredString(W/2, 2.5*mm, "競馬予想AI  v8.0  |  本レポートは参考情報です")

    doc = RacingDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=R_MARGIN,
        leftMargin=L_MARGIN,
        topMargin=T_MARGIN + 8*mm,
        bottomMargin=B_MARGIN + 6*mm,
        report_venue=venue,
        report_date=date_formatted,
        report_type=report_type,
    )

    frame = Frame(
        L_MARGIN, B_MARGIN + 6*mm,
        W - L_MARGIN - R_MARGIN,
        H - T_MARGIN - 8*mm - B_MARGIN - 6*mm,
        id='main'
    )
    doc.addPageTemplates([PageTemplate(id='main', frames=frame)])

    story = []

    for race in batch_data:
        df          = race['df']
        race_no     = race['no']
        race_name   = race.get('name', '')
        race_info   = race.get('info', {})
        track_type  = race_info.get('track_type', '') if race_info else ''
        distance    = race_info.get('distance', '')   if race_info else ''
        cond_str    = f"{track_type}  {distance}m" if distance else ''

        # ---- レースヘッダーバー ----
        heading_txt  = f"{race_no}R  {race_name}"
        cond_txt     = cond_str

        header_table = Table(
            [[
                Paragraph(heading_txt, race_heading_style),
                Paragraph(cond_txt,    condition_style),
            ]],
            colWidths=[100*mm, None],
            hAlign='LEFT',
        )
        header_table.setStyle(TableStyle([
            ('BACKGROUND',    (0, 0), (-1, -1), PDF_ACCENT),
            ('TOPPADDING',    (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('LEFTPADDING',   (0, 0), (-1, -1), 6),
            ('RIGHTPADDING',  (0, 0), (-1, -1), 6),
            ('VALIGN',        (0, 0), (-1, -1), 'MIDDLE'),
            ('ALIGN',         (1, 0), (1, 0),   'RIGHT'),
            ('LINEBELOW',     (0, 0), (-1, -1), 2.0, PDF_GOLD),
        ]))

        # ---- データテーブル ----
        if not df.empty:
            data_tbl = _make_race_table(df, FN, mode)
        else:
            data_tbl = Paragraph("データなし", nodata_style)

        race_block = KeepTogether([
            header_table,
            Spacer(1, 1*mm),
            data_tbl,
            Spacer(1, 5*mm),
        ])
        story.append(race_block)

        if race_no % 4 == 0 and race_no < max(r['no'] for r in batch_data):
            story.append(PageBreak())

    doc.build(story)
    buffer.seek(0)
    return buffer

# --- セッション初期化 ---
if 'batch_data' not in st.session_state: 
    st.session_state['batch_data'] = []
if 'race_info' not in st.session_state: 
    st.session_state['race_info'] = None
if 'res_map' not in st.session_state: 
    st.session_state['res_map'] = {}
if 'current_mode' not in st.session_state:
    st.session_state['current_mode'] = 'analysis'

# --- 定数定義 ---
VENUES = {
    "東京": "05",
    "阪神": "09",
    "小倉": "10"
}


SCHEDULE = {
    "20260221": {
        "東京": (1, 7),
        "阪神": (1, 1),
        "小倉": (1, 9),
    },
    "20260222": {
        "東京": (1, 8),
        "阪神": (1, 2),
        "小倉": (1, 10),
    }
}


# --- サイドバー ---
with st.sidebar:
    st.header("⚙️ 設定")
    mode = st.selectbox("📌 モード", ["個別レース", "一括レース"])
    date_sel = st.selectbox("開催日", list(SCHEDULE.keys()))
    venue_sel = st.selectbox("競馬場", list(SCHEDULE[date_sel].keys()))
    
    # 全変数をデフォルトFalseで初期化（モード切替時のNameError防止）
    analyze_clicked = False
    result_clicked = False
    batch_analyze_clicked = False
    batch_result_clicked = False
    race_no = 11  # デフォルト値

    if mode == "個別レース":
        race_no = st.selectbox("レース番号", range(1, 13), index=10)
        analyze_clicked = st.button("🚀 指数分析", type="primary", use_container_width=True)
        result_clicked = st.button("🏆 結果照合", use_container_width=True)
    else:
        batch_analyze_clicked = st.button("🚀 全レース一括解析", type="primary", use_container_width=True)
        batch_result_clicked = st.button("🏆 全レース結果照合", use_container_width=True)

# --- メインコンテンツ ---
st.title("🏇 競馬予想AI v9.0")

# --- 1. 個別解析ロジック ---
if mode == "個別レース":
    rid = f"{date_sel[:4]}{VENUES[venue_sel]}{SCHEDULE[date_sel][venue_sel][0]:02d}{SCHEDULE[date_sel][venue_sel][1]:02d}{race_no:02d}"
    
    if analyze_clicked:
        progress_placeholder = st.empty()
        status_placeholder = st.empty()
        
        with st.spinner(f"{race_no}R 分析中..."):
            # 進捗表示用のコールバック関数
            def progress_callback(horse_name, current, total):
                percent = int((current / total) * 100)
                progress_placeholder.progress(percent / 100)
                status_placeholder.text(f"🔍 {horse_name} を分析中... ({current}/{total})")
            
            scraper = NetkeibaRaceScraper()
            scraper.progress_callback = progress_callback
            st.session_state['race_info'] = scraper.get_race_data(rid)
            st.session_state['res_map'] = {}  # 照合はリセット
            st.session_state['current_mode'] = 'analysis'
        
        progress_placeholder.empty()
        status_placeholder.empty()

    if result_clicked:
        with st.spinner("結果取得中..."):
            st.session_state['res_map'] = fetch_results_simple(rid)
            st.session_state['current_mode'] = 'result'

    if st.session_state['race_info']:
        info = st.session_state['race_info']
        
        # 取りやめレース・新馬戦の表示
        if info.get('is_cancelled'):
            st.warning(f"⚠️ このレースは取りやめになりました")
            st.info(f"理由: {info.get('skip_reason', 'レース取りやめ')}")
            st.stop()
        elif info.get('is_new_horse_race'):
            st.info(f"ℹ️ このレースは新馬戦のためスキップされました")
            st.info(f"理由: {info.get('skip_reason', '新馬戦')}")
            st.stop()
        elif info.get('is_障害_race'):
            st.warning(f"🚧 このレースは障害レースのため予想対象外です")
            st.stop()
        
        # レース名と条件を表示
        race_title = f"📋 {race_no}R {info.get('race_name', '')}"
        race_condition = f"({info.get('track_type', '')} {info.get('distance', '')}m)"
        
        st.subheader(f"{race_title} {race_condition}")
        
        df = prepare_display_df(info['df'], st.session_state['res_map'])
        st.dataframe(df, hide_index=True, use_container_width=True)
        
        # PDF出力ボタン
        if not df.empty:
            st.markdown("---")
            pdf_data = [{
                'no': race_no, 
                'name': info.get('race_name', ''),
                'info': info,
                'df': df
            }]
            
            pdf_buffer = create_pdf_report(
                pdf_data, 
                venue_sel, 
                date_sel,
                mode=st.session_state['current_mode']
            )
            
            report_type = "予想レポート" if st.session_state['current_mode'] == 'analysis' else "結果照合レポート"
            filename = f"{venue_sel}_{race_no}R_{report_type}_{datetime.now().strftime('%Y%m%d')}.pdf"
            
            st.download_button(
                label=f"📥 {report_type}をダウンロード (PDF)",
                data=pdf_buffer,
                file_name=filename,
                mime="application/pdf",
                use_container_width=True
            )

# --- 2. 一括解析ロジック ---
elif mode == "一括レース":
    if batch_analyze_clicked or batch_result_clicked:
        st.session_state['batch_data'] = []
        st.session_state['current_mode'] = 'result' if batch_result_clicked else 'analysis'
        
        # 進捗表示用のプレースホルダー
        race_progress_bar = st.progress(0)
        race_status = st.empty()
        horse_progress_bar = st.progress(0)
        horse_status = st.empty()
        
        scraper = NetkeibaRaceScraper()
        
        # 馬の進捗表示用コールバック
        def progress_callback(horse_name, current, total):
            percent = int((current / total) * 100)
            horse_progress_bar.progress(percent / 100)
            horse_status.text(f"   🐴 {horse_name} を分析中... ({current}/{total}頭)")
        
        scraper.progress_callback = progress_callback
        
        for i in range(1, 13):
            rid = f"{date_sel[:4]}{VENUES[venue_sel]}{SCHEDULE[date_sel][venue_sel][0]:02d}{SCHEDULE[date_sel][venue_sel][1]:02d}{i:02d}"
            
            # レース進捗を表示
            race_percent = int(((i - 1) / 12) * 100)
            race_progress_bar.progress(race_percent / 100)
            race_status.markdown(f"### 📊 {i}R を解析中... ({i}/12レース)")
            
            try:
                res = scraper.get_race_data(rid)
                
                # 取りやめレース・新馬戦をスキップ
                if res and res.get('is_cancelled'):
                    st.warning(f"⚠️ {i}R: {res.get('skip_reason', 'レース取りやめ')} - スキップします")
                    continue
                elif res and res.get('is_new_horse_race'):
                    st.info(f"ℹ️ {i}R: {res.get('skip_reason', '新馬戦')} - スキップします")
                elif res and res.get('is_障害_race'):
                    st.warning(f"🚧 {i}R: 障害レース - スキップします")
                    continue
                
                if res and not res['df'].empty:
                    rmap = fetch_results_simple(rid) if batch_result_clicked else {}
                    df_res = prepare_display_df(res['df'], rmap)
                    st.session_state['batch_data'].append({
                        'no': i, 
                        'name': res.get('race_name', ''),
                        'info': res,
                        'df': df_res
                    })
            except Exception as e:
                st.error(f"❌ {i}R: エラーが発生しました - {str(e)[:100]} - スキップします")
                continue
            
            # レース完了時にプログレスバーを100%に
            race_progress_bar.progress(i / 12)
            horse_progress_bar.progress(0)
            horse_status.empty()
        
        # 完了後にプレースホルダーをクリア
        race_progress_bar.empty()
        race_status.empty()
        horse_progress_bar.empty()
        horse_status.empty()
        
        st.success("✅ 全レースの解析が完了しました!")

    if st.session_state['batch_data']:
        # レース一覧表示
        for race in st.session_state['batch_data']:
            race_condition = f"({race['info'].get('track_type', '')} {race['info'].get('distance', '')}m)"
            st.markdown(f"#### {race['no']}R {race['name']} {race_condition}")
            st.dataframe(race['df'], hide_index=True, use_container_width=True)
        
        # PDF出力ボタン(全レース)
        st.markdown("---")
        
        pdf_buffer = create_pdf_report(
            st.session_state['batch_data'], 
            venue_sel, 
            date_sel,
            mode=st.session_state['current_mode']
        )
        
        report_type = "全レース予想" if st.session_state['current_mode'] == 'analysis' else "全レース結果照合"
        filename = f"{venue_sel}_{report_type}_{datetime.now().strftime('%Y%m%d')}.pdf"
        
        st.download_button(
            label=f"📥 {report_type}レポートをダウンロード (PDF)",
            data=pdf_buffer,
            file_name=filename,
            mime="application/pdf",
            type="primary",
            use_container_width=True
        )
