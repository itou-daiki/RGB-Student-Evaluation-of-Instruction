"""
授業アンケートデータ処理モジュール

CSVデータの読み込み、クレンジング、スコアリング、集計を行う関数群
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import io


# スコアリング変換マッピング（4件法）
SCORE_MAPPING = {
    # 4点（最高評価）
    "とてもそう思う": 4,
    "当てはまる": 4,
    "とても当てはまる": 4,
    "強くそう思う": 4,

    # 3点
    "そう思う": 3,
    "どちらかといえばそう思う": 3,
    "やや当てはまる": 3,
    "どちらかといえば当てはまる": 3,

    # 2点
    "あまりそう思わない": 2,
    "どちらかといえばそう思わない": 2,
    "あまり当てはまらない": 2,
    "どちらかといえば当てはまらない": 2,

    # 1点（最低評価）
    "思わない": 1,
    "そう思わない": 1,
    "当てはまらない": 1,
    "全く当てはまらない": 1,
    "全くそう思わない": 1,
}


# 除外するメタデータカラム名（これらは質問項目として扱わない）
METADATA_COLUMNS = [
    "Id",
    "id",
    "ID",
    "開始時刻",
    "完了時刻",
    "メール",
    "メールアドレス",
    "Email",
    "email",
    "名前",
    "氏名",
    "Name",
    "name",
    "タイムスタンプ",
    "Timestamp",
    "timestamp",
]

# 必須カラム（科目名識別用）
SUBJECT_COLUMN_PATTERNS = [
    "科目名",
    "科目",
    "教科名",
    "授業名",
]

# 出席番号カラムのパターン
STUDENT_ID_PATTERNS = [
    "出席番号",
    "学籍番号",
    "学生番号",
]

# 自由記述カラムのパターン
FREE_TEXT_PATTERNS = [
    "意見・感想",
    "意見",
    "感想",
    "コメント",
    "自由記述",
    "その他",
    "記入してください",
    "ご記入ください",
]


def convert_to_score(value: str) -> Optional[float]:
    """
    4件法のテキスト回答を数値スコアに変換

    Args:
        value: 回答テキスト

    Returns:
        float: 変換後のスコア（1-4）、変換できない場合はNone
    """
    if pd.isna(value):
        return None

    # 文字列に変換して前後の空白を削除
    value_str = str(value).strip()

    # マッピングテーブルから検索
    return SCORE_MAPPING.get(value_str, None)


def detect_subject_column(df: pd.DataFrame) -> Optional[str]:
    """
    科目名カラムを自動検出

    Args:
        df: データフレーム

    Returns:
        str: 科目名カラム名、見つからない場合はNone
    """
    for col in df.columns:
        for pattern in SUBJECT_COLUMN_PATTERNS:
            if pattern in col:
                return col
    return None


def detect_student_id_column(df: pd.DataFrame) -> Optional[str]:
    """
    出席番号カラムを自動検出

    Args:
        df: データフレーム

    Returns:
        str: 出席番号カラム名、見つからない場合はNone
    """
    for col in df.columns:
        for pattern in STUDENT_ID_PATTERNS:
            if pattern in col:
                return col
    return None


def detect_free_text_column(df: pd.DataFrame) -> Optional[str]:
    """
    自由記述カラムを自動検出

    Args:
        df: データフレーム

    Returns:
        str: 自由記述カラム名、見つからない場合はNone
    """
    # より具体的なパターンから順に検索
    # （「意見」だけでなく「ご記入ください」などの具体的なパターンを優先）
    priority_patterns = ["ご記入ください", "記入してください", "意見・感想", "自由記述"]

    # 優先パターンで検索
    for pattern in priority_patterns:
        for col in df.columns:
            if pattern in col:
                return col

    # その他のパターンで検索
    for col in df.columns:
        for pattern in FREE_TEXT_PATTERNS:
            if pattern in col and pattern not in priority_patterns:
                return col

    return None


def identify_question_columns(df: pd.DataFrame) -> List[str]:
    """
    質問項目カラムを識別（メタデータ、科目名、出席番号、自由記述を除外）

    Args:
        df: データフレーム

    Returns:
        List[str]: 質問項目カラムのリスト
    """
    # 除外カラムを特定
    exclude_cols = set()

    # メタデータカラム
    for col in df.columns:
        if col in METADATA_COLUMNS:
            exclude_cols.add(col)

    # 科目名カラム
    subject_col = detect_subject_column(df)
    if subject_col:
        exclude_cols.add(subject_col)

    # 出席番号カラム
    student_id_col = detect_student_id_column(df)
    if student_id_col:
        exclude_cols.add(student_id_col)

    # 自由記述カラム
    free_text_col = detect_free_text_column(df)
    if free_text_col:
        exclude_cols.add(free_text_col)

    # 質問項目カラムを抽出
    question_cols = [col for col in df.columns if col not in exclude_cols]

    return question_cols


def load_and_process_csv(uploaded_file) -> Tuple[pd.DataFrame, Dict]:
    """
    アップロードされたCSVまたはExcelファイルを読み込み、処理する

    Args:
        uploaded_file: StreamlitのUploadedFileオブジェクト

    Returns:
        Tuple[pd.DataFrame, Dict]: 処理済みデータフレームとメタデータ
    """
    # ファイル名から拡張子を取得
    file_name = uploaded_file.name if hasattr(uploaded_file, 'name') else ''
    file_extension = file_name.lower().split('.')[-1] if '.' in file_name else ''

    # ExcelファイルかCSVファイルかを判定して読み込み
    if file_extension in ['xlsx', 'xls']:
        # Excelファイルを読み込み
        df = pd.read_excel(uploaded_file, engine='openpyxl')
    else:
        # CSVを読み込み（エンコーディングを自動判定）
        try:
            df = pd.read_csv(uploaded_file, encoding='utf-8-sig')
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(uploaded_file, encoding='shift-jis')
            except UnicodeDecodeError:
                df = pd.read_csv(uploaded_file, encoding='cp932')

    # カラム検出
    subject_col = detect_subject_column(df)
    student_id_col = detect_student_id_column(df)
    free_text_col = detect_free_text_column(df)
    question_cols = identify_question_columns(df)

    # メタデータを作成
    metadata = {
        'subject_column': subject_col,
        'student_id_column': student_id_col,
        'free_text_column': free_text_col,
        'question_columns': question_cols,
        'total_responses': len(df),
    }

    return df, metadata


def calculate_statistics(df: pd.DataFrame, question_cols: List[str]) -> pd.DataFrame:
    """
    質問項目ごとの統計情報を計算

    Args:
        df: データフレーム
        question_cols: 質問項目カラムのリスト

    Returns:
        pd.DataFrame: 統計情報（質問、平均値、各スコアの回答数）
    """
    stats_list = []

    for question in question_cols:
        # スコアに変換
        scores = df[question].apply(convert_to_score)

        # 有効な回答数
        valid_count = scores.notna().sum()

        if valid_count == 0:
            continue

        # 平均値
        mean_score = scores.mean()

        # 各スコアの分布
        score_counts = scores.value_counts().sort_index()
        count_4 = score_counts.get(4.0, 0)
        count_3 = score_counts.get(3.0, 0)
        count_2 = score_counts.get(2.0, 0)
        count_1 = score_counts.get(1.0, 0)

        stats_list.append({
            '質問項目': question,
            '平均値': mean_score,
            '有効回答数': valid_count,
            '4点の回答数': int(count_4),
            '3点の回答数': int(count_3),
            '2点の回答数': int(count_2),
            '1点の回答数': int(count_1),
        })

    return pd.DataFrame(stats_list)


def get_overall_average(df: pd.DataFrame, question_cols: List[str]) -> float:
    """
    全質問の総合平均点を計算

    Args:
        df: データフレーム
        question_cols: 質問項目カラムのリスト

    Returns:
        float: 総合平均点
    """
    all_scores = []

    for question in question_cols:
        scores = df[question].apply(convert_to_score)
        all_scores.extend(scores.dropna().tolist())

    if len(all_scores) == 0:
        return 0.0

    return np.mean(all_scores)


def extract_free_comments(df: pd.DataFrame, free_text_col: str,
                         exclude_empty: bool = True) -> List[str]:
    """
    自由記述を抽出

    Args:
        df: データフレーム
        free_text_col: 自由記述カラム名
        exclude_empty: 空白や「特になし」を除外するか

    Returns:
        List[str]: 自由記述のリスト
    """
    if not free_text_col or free_text_col not in df.columns:
        return []

    comments = df[free_text_col].tolist()

    if exclude_empty:
        # 空白、NaN、「特になし」「特にありません」などを除外
        exclude_patterns = ['特になし', '特にありません', 'なし', '無し', '']
        comments = [
            str(c).strip() for c in comments
            if pd.notna(c) and str(c).strip() not in exclude_patterns
        ]

    return comments


def create_download_data(stats_df: pd.DataFrame, overall_avg: float,
                        subject_name: str = "全体") -> pd.DataFrame:
    """
    ダウンロード用のExcelデータを作成

    Args:
        stats_df: 統計データフレーム
        overall_avg: 総合平均点
        subject_name: 科目名

    Returns:
        pd.DataFrame: ダウンロード用データ
    """
    # データをコピー
    download_df = stats_df.copy()

    # パーセンテージカラムを追加
    download_df['4点の割合(%)'] = (
        download_df['4点の回答数'] / download_df['有効回答数'] * 100
    ).round(1)
    download_df['3点の割合(%)'] = (
        download_df['3点の回答数'] / download_df['有効回答数'] * 100
    ).round(1)
    download_df['2点の割合(%)'] = (
        download_df['2点の回答数'] / download_df['有効回答数'] * 100
    ).round(1)
    download_df['1点の割合(%)'] = (
        download_df['1点の回答数'] / download_df['有効回答数'] * 100
    ).round(1)

    # 平均値を小数点2桁に丸める
    download_df['平均値'] = download_df['平均値'].round(2)

    # カラムの順序を整理
    download_df = download_df[[
        '質問項目',
        '平均値',
        '有効回答数',
        '4点の回答数',
        '4点の割合(%)',
        '3点の回答数',
        '3点の割合(%)',
        '2点の回答数',
        '2点の割合(%)',
        '1点の回答数',
        '1点の割合(%)',
    ]]

    return download_df


def write_to_template(df: pd.DataFrame, question_cols: List[str],
                     subject_mapping: Optional[Dict[str, List[str]]] = None,
                     placeholders: Optional[Dict[str, str]] = None,
                     template_path: str = "テンプレート.xlsx") -> io.BytesIO:
    """
    テンプレートExcelファイルに各教科のデータを書き込む

    Args:
        df: 全データを含むデータフレーム
        question_cols: 質問項目カラムのリスト
        subject_mapping: ユーザーが選択した教科と科目のマッピング（教科名 -> 科目名リスト）
        placeholders: テンプレートのプレースホルダー（{Y}, {n}, {MM}など）とその値
        template_path: テンプレートファイルのパス

    Returns:
        BytesIO: 書き込み済みExcelファイル
    """
    import openpyxl
    from openpyxl.utils import get_column_letter
    import re

    # テンプレートファイルを読み込む
    wb = openpyxl.load_workbook(template_path)
    ws = wb['概要']

    # テンプレートの6行目から質問項目の名前を読み込む（C列からAF列まで）
    template_questions = []
    for col_idx in range(3, 33):  # C列（3）からAF列（32）まで
        cell = ws.cell(row=6, column=col_idx)
        if cell.value:
            template_questions.append((col_idx, str(cell.value)))

    print(f"🔍 テンプレートの質問項目: {len(template_questions)}個")

    # プレースホルダーを置換する関数
    def replace_placeholders(text):
        if placeholders and text:
            for key, value in placeholders.items():
                text = text.replace(f'{{{key}}}', str(value))
        return text

    # 1行目と2行目のプレースホルダーを置換
    for row_idx in [1, 2]:
        for col_idx in range(1, ws.max_column + 1):
            cell = ws.cell(row=row_idx, column=col_idx)
            if cell.value and isinstance(cell.value, str):
                cell.value = replace_placeholders(cell.value)

    # 7行目から16行目のB列（質問項目列）のプレースホルダーを置換
    for row_idx in range(7, 17):
        cell = ws.cell(row=row_idx, column=2)  # B列
        if cell.value and isinstance(cell.value, str):
            cell.value = replace_placeholders(cell.value)

    # 教科名のマッピング（テンプレートの行番号）
    subject_row_mapping = {
        '全体': 7,
        '国語': 8,
        '数学': 9,
        '地歴公民': 10,
        '理科': 11,
        '外国語': 12,
        '保健体育': 13,
        '芸術': 14,
        '家庭': 15,
        '情報': 16,
    }

    # 科目カラムを検出
    subject_col = detect_subject_column(df)

    # 全体の統計を計算
    overall_stats = calculate_statistics(df, question_cols)

    # デバッグ: 質問項目数と統計データの確認
    print(f"🔍 デバッグ情報 (write_to_template):")
    print(f"  - 質問項目数: {len(question_cols)}")
    print(f"  - 統計データの行数: {len(overall_stats)}")
    print(f"  - 質問項目リスト (最初の5個): {question_cols[:5]}")
    if len(question_cols) > 30:
        print(f"  ⚠️ 警告: 質問項目が30個を超えています。余分な項目:")
        print(f"    {question_cols[30:]}")

    # 質問項目のマッピングを作成
    # アップロードされたデータの質問項目とテンプレートの質問項目を照合
    question_mapping = {}  # {テンプレート列番号: データの質問項目インデックス}

    for template_col_idx, template_question in template_questions:
        matched = False

        # まず完全一致を探す
        for data_idx, data_question in enumerate(question_cols):
            if data_question == template_question:
                question_mapping[template_col_idx] = data_idx
                matched = True
                break

        # 完全一致がない場合、部分一致を試す
        if not matched:
            for data_idx, data_question in enumerate(question_cols):
                # テンプレート質問がデータ質問に含まれているか
                if template_question in data_question:
                    question_mapping[template_col_idx] = data_idx
                    matched = True
                    break
                # データ質問がテンプレート質問に含まれているか
                elif data_question in template_question:
                    question_mapping[template_col_idx] = data_idx
                    matched = True
                    break

    print(f"  - マッピングされた質問項目: {len(question_mapping)}個 / {len(template_questions)}個")

    # マッピングされなかった質問項目を警告
    unmapped_template = [q for col_idx, q in template_questions if col_idx not in question_mapping]
    if unmapped_template:
        print(f"  ⚠️ 警告: 以下のテンプレート質問項目にデータが見つかりませんでした:")
        for q in unmapped_template[:5]:  # 最初の5個のみ表示
            print(f"    - {q}")
        if len(unmapped_template) > 5:
            print(f"    ... 他{len(unmapped_template) - 5}個")

    # 全体データを書き込み（7行目）
    row_idx = subject_row_mapping['全体']
    avg_values = overall_stats['平均値'].tolist()

    for template_col_idx, data_idx in question_mapping.items():
        if data_idx < len(avg_values):
            ws.cell(row=row_idx, column=template_col_idx, value=round(avg_values[data_idx], 2))

    # 各教科のデータを処理して書き込む
    if subject_col and subject_col in df.columns:
        # ユーザーが選択したマッピングがある場合はそれを使用
        if subject_mapping:
            # ユーザー選択のマッピングを使用
            for template_subject, row_idx in subject_row_mapping.items():
                if template_subject == '全体':
                    continue

                # ユーザーが選択した科目のリストを取得
                matched_subjects = subject_mapping.get(template_subject, [])

                # マッチした教科のデータをフィルタリング
                if matched_subjects:
                    subject_df = df[df[subject_col].isin(matched_subjects)]

                    if len(subject_df) == 0:
                        continue

                    # 統計を計算
                    subject_stats = calculate_statistics(subject_df, question_cols)

                    # データを書き込み（質問項目のマッピングを使用）
                    avg_values = subject_stats['平均値'].tolist()
                    for template_col_idx, data_idx in question_mapping.items():
                        if data_idx < len(avg_values):
                            ws.cell(row=row_idx, column=template_col_idx, value=round(avg_values[data_idx], 2))
        else:
            # デフォルトの自動マッピング（後方互換性のため）
            # 教科名の部分一致用キーワード
            subject_keywords = {
                '国語': ['国語', 'こくご'],
                '数学': ['数学', 'すうがく'],
                '地歴公民': ['地理', '歴史', '公民', '地歴', '社会'],
                '理科': ['理科', '物理', '化学', '生物', '地学'],
                '外国語': ['英語', '外国語', 'English'],
                '保健体育': ['保健', '体育', 'たいいく'],
                '芸術': ['音楽', '美術', '書道', '芸術'],
                '家庭': ['家庭', 'かてい'],
                '情報': ['情報', 'じょうほう'],
            }

            # データに含まれる教科名を取得
            unique_subjects = df[subject_col].unique()

            # 各テンプレート教科について処理
            for template_subject, row_idx in subject_row_mapping.items():
                if template_subject == '全体':
                    continue

                # 部分一致で教科を検索
                matched_subjects = []
                keywords = subject_keywords.get(template_subject, [])

                for actual_subject in unique_subjects:
                    if pd.isna(actual_subject):
                        continue

                    actual_subject_str = str(actual_subject)

                    # 完全一致をチェック
                    if actual_subject_str == template_subject:
                        matched_subjects.append(actual_subject)
                        continue

                    # キーワードによる部分一致をチェック
                    for keyword in keywords:
                        if keyword in actual_subject_str:
                            matched_subjects.append(actual_subject)
                            break

                # マッチした教科のデータをフィルタリング
                if matched_subjects:
                    subject_df = df[df[subject_col].isin(matched_subjects)]

                    if len(subject_df) == 0:
                        continue

                    # 統計を計算
                    subject_stats = calculate_statistics(subject_df, question_cols)

                    # データを書き込み（質問項目のマッピングを使用）
                    avg_values = subject_stats['平均値'].tolist()
                    for template_col_idx, data_idx in question_mapping.items():
                        if data_idx < len(avg_values):
                            ws.cell(row=row_idx, column=template_col_idx, value=round(avg_values[data_idx], 2))

    # BytesIOに書き込み
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    return output
