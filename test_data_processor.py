"""
data_processorモジュールの簡易テスト
"""

import pandas as pd
from data_processor import (
    load_and_process_csv,
    calculate_statistics,
    get_overall_average,
    extract_free_comments,
)


def test_data_processing():
    """データ処理機能のテスト"""
    print("データ処理のテストを開始します...")

    # サンプルファイルを読み込み
    sample_file = "sample_data/survey_数学I.csv"
    print(f"\n1. ファイル読み込みテスト: {sample_file}")

    # ファイルを開いて処理
    with open(sample_file, 'rb') as f:
        df, metadata = load_and_process_csv(f)

    print(f"   ✓ 読み込み成功")
    print(f"   - 回答数: {len(df)}件")
    print(f"   - 科目カラム: {metadata['subject_column']}")
    print(f"   - 質問項目数: {len(metadata['question_columns'])}項目")
    print(f"   - 自由記述カラム: {metadata['free_text_column']}")

    # 統計情報を計算
    print("\n2. 統計計算テスト")
    stats_df = calculate_statistics(df, metadata['question_columns'])
    print(f"   ✓ 統計計算成功")
    print(f"   - 集計された質問数: {len(stats_df)}項目")

    # 先頭3項目を表示
    print("\n   上位3項目の統計:")
    for idx, row in stats_df.head(3).iterrows():
        print(f"   - {row['質問項目'][:30]}... : 平均 {row['平均値']:.2f}点")

    # 総合平均を計算
    print("\n3. 総合平均計算テスト")
    overall_avg = get_overall_average(df, metadata['question_columns'])
    print(f"   ✓ 総合平均: {overall_avg:.2f}点")

    # 自由記述を抽出
    print("\n4. 自由記述抽出テスト")
    comments = extract_free_comments(
        df,
        metadata['free_text_column'],
        exclude_empty=True
    )
    print(f"   ✓ 有効なコメント数: {len(comments)}件")
    if comments:
        print(f"   - サンプル: {comments[0][:50]}...")

    print("\n✅ すべてのテストが成功しました！")


if __name__ == "__main__":
    test_data_processing()
