"""
`data_processor`モジュールのテスト
"""

import pandas as pd
import pytest
import numpy as np
from data_processor import (
    convert_to_score,
    detect_subject_column,
    detect_student_id_column,
    detect_free_text_column,
    identify_question_columns,
    calculate_statistics,
    extract_free_comments,
)

# テスト用のサンプルデータを作成するフィクスチャ
@pytest.fixture
def sample_df() -> pd.DataFrame:
    """テスト用のサンプルDataFrameを生成する"""
    data = {
        "科目名": ["数学I", "数学I", "数学I", "数学I", "英語I"],
        "学籍番号": ["1001", "1002", "1003", "1004", "1001"],
        "授業の内容を理解できている": ["とてもそう思う", "そう思う", "あまりそう思わない", "思わない", "とてもそう思う"],
        "授業のスピードは適切である": ["そう思う", "そう思う", "どちらかといえばそう思わない", pd.NA, "そう思う"],
        "この授業に関して、意見や感想があれば記入してください": ["特になし", "面白いです", "", "改善点はありません", "楽しかった"]
    }
    return pd.DataFrame(data)

# --- convert_to_score関数のテスト ---
@pytest.mark.parametrize("text, expected", [
    ("とてもそう思う", 4),
    ("そう思う", 3),
    ("あまりそう思わない", 2),
    ("思わない", 1),
    ("当てはまる", 4),
    ("どちらかといえば当てはまる", 3),
    ("全く当てはまらない", 1),
    ("無関係なテキスト", None),
    ("", None),
    (None, None),
    (np.nan, None),
])
def test_convert_to_score(text, expected):
    """テキストからスコアへの変換をテストする"""
    assert convert_to_score(text) == expected

# --- カラム検出関数のテスト ---
def test_detect_subject_column(sample_df):
    """科目名カラムの検出をテストする"""
    assert detect_subject_column(sample_df) == "科目名"

def test_detect_student_id_column(sample_df):
    """学籍番号カラムの検出をテストする"""
    assert detect_student_id_column(sample_df) == "学籍番号"

def test_detect_free_text_column(sample_df):
    """自由記述カラムの検出をテストする"""
    assert detect_free_text_column(sample_df) == "この授業に関して、意見や感想があれば記入してください"

def test_identify_question_columns(sample_df):
    """質問項目カラムの識別をテストする"""
    expected = ["授業の内容を理解できている", "授業のスピードは適切である"]
    assert identify_question_columns(sample_df) == expected

# --- calculate_statistics関数のテスト ---
def test_calculate_statistics(sample_df):
    """統計計算機能をテストする"""
    question_cols = ["授業の内容を理解できている", "授業のスピードは適切である"]
    stats = calculate_statistics(sample_df, question_cols)

    assert len(stats) == 2
    assert "質問項目" in stats.columns
    assert "平均値" in stats.columns
    assert "有効回答数" in stats.columns

    # "授業の内容を理解できている" の統計を検証 (スコア: 4, 3, 2, 1, 4)
    q1_stats = stats[stats["質問項目"] == "授業の内容を理解できている"].iloc[0]
    assert q1_stats["有効回答数"] == 5
    assert q1_stats["平均値"] == pytest.approx((4 + 3 + 2 + 1 + 4) / 5)
    assert q1_stats["4点の回答数"] == 2
    assert q1_stats["1点の回答数"] == 1

    # "授業のスピードは適切である" の統計を検証 (スコア: 3, 3, 2, NA, 3)
    q2_stats = stats[stats["質問項目"] == "授業のスピードは適切である"].iloc[0]
    assert q2_stats["有効回答数"] == 4
    assert q2_stats["平均値"] == pytest.approx((3 + 3 + 2 + 3) / 4)
    assert q2_stats["3点の回答数"] == 3
    assert q2_stats["2点の回答数"] == 1

# --- extract_free_comments関数のテスト ---
def test_extract_free_comments(sample_df):
    """自由記述の抽出をテストする"""
    comment_col = "この授業に関して、意見や感想があれば記入してください"
    
    # 空コメントを除外する場合 (デフォルト)
    comments_excluded = extract_free_comments(sample_df, comment_col)
    assert comments_excluded == ["面白いです", "改善点はありません", "楽しかった"]
    
    # 空コメントを含める場合
    comments_included = extract_free_comments(sample_df, comment_col, exclude_empty=False)
    assert comments_included == ["特になし", "面白いです", "", "改善点はありません", "楽しかった"]

def test_extract_free_comments_no_column():
    """自由記述カラムが存在しない場合のテスト"""
    df = pd.DataFrame({'A': [1, 2]})
    assert extract_free_comments(df, "non_existent_column") == []