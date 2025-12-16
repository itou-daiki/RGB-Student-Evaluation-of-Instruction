"""
授業アンケートのサンプルデータ生成スクリプト

このスクリプトは、授業アンケート分析アプリのテスト用ダミーデータを生成します。
"""

import pandas as pd
import random
from pathlib import Path

# 科目名のリスト
SUBJECTS = [
    "数学I",
    "英語コミュニケーションI",
    "現代の国語",
    "物理基礎",
    "化学基礎",
]

# 4件法の回答選択肢
FOUR_POINT_SCALE = [
    "とてもそう思う",
    "そう思う",
    "あまりそう思わない",
    "思わない",
]

# 質問項目リスト
QUESTIONS = [
    "授業の内容を理解できている",
    "先生の授業は分かりやすい",
    "授業の目標を把握して授業を受けている",
    "先生の発問や学習課題で思考が深まる",
    "教えあい・意見交換の場が設けられている",
    "板書やスライド、プリントで学習の流れや重点がつかめる",
    "授業の最後に振り返りの時間が設けられている",
    "この授業では、「なぜ」を考える場面が設けられている",
    "課題や宿題の量は適切である",
    "授業のスピードは適切である",
    "先生の説明は聞き取りやすい",
    "授業に集中できる雰囲気がある",
    "教科書や教材は効果的に使われている",
    "ICTや視聴覚教材が効果的に活用されている",
    "グループ活動やペアワークが効果的である",
    "実験や実習が理解を深めるのに役立っている",
    "小テストや確認テストが学習の定着に役立っている",
    "先生の指導は熱意が感じられる",
    "質問しやすい雰囲気がある",
    "授業で学んだことを実生活や将来に活かせると思う",
    "この授業で新しい発見や気づきがある",
    "授業の内容に興味・関心が持てる",
    "自分から積極的に授業に参加している",
    "予習・復習に取り組んでいる",
    "ノートやワークシートを丁寧に作成している",
    "提出物は期限内に出している",
    "授業で分からないことは質問や自習で解決している",
    "授業を通じて考える力が身についている",
    "授業を通じて表現する力が身についている",
    "授業を通じて自ら学ぶ姿勢が身についている",
]

# 評価に関する質問
EVALUATION_QUESTIONS = [
    "この授業における、自分の知識・技能面への評価(ABC)は適切ですか?",
    "この授業における、自分の思考・判断・表現への評価(ABC)は適切ですか?",
    "この授業における、自分の主体的に学習に取り組む態度面への評価(ABC)は適切ですか?",
]

# 自由記述のサンプル
FREE_COMMENTS = [
    "とても分かりやすい授業でした。これからも頑張ります。",
    "もう少しゆっくり進めてほしいです。",
    "グループワークが楽しく、理解が深まりました。",
    "先生の説明が丁寧で良かったです。",
    "もっと実験の時間を増やしてほしいです。",
    "特になし",
    "",
    "質問しやすい雰囲気で助かっています。",
    "予習が大変ですが、力がついていると実感しています。",
    "授業が面白くて、この科目が好きになりました。",
    "",
    "もう少し演習問題を解く時間がほしいです。",
    "ICTの活用が効果的で分かりやすいです。",
    "特にありません",
    "定期テストの範囲が広いので、もう少し絞ってほしいです。",
]


def generate_sample_data(num_responses: int = 50, subject: str = None) -> pd.DataFrame:
    """
    サンプルアンケートデータを生成する

    Args:
        num_responses: 生成する回答数
        subject: 科目名（指定しない場合はランダム）

    Returns:
        pd.DataFrame: 生成されたアンケートデータ
    """
    data = []

    for i in range(num_responses):
        # 出席番号を生成（1001から連番）
        student_id = f"{1001 + i:04d}"

        # 科目名を決定
        if subject:
            subject_name = subject
        else:
            subject_name = random.choice(SUBJECTS)

        # 1行分のデータを作成
        row = {
            "科目名を選択してください": subject_name,
            "出席番号を4桁の数字で入力してください": student_id,
        }

        # 質問項目に対する回答を生成
        # 重み付けで肯定的な回答が多くなるようにする
        weights = [0.4, 0.4, 0.15, 0.05]  # とてもそう思う、そう思う、あまりそう思わない、思わない

        for question in QUESTIONS:
            row[question] = random.choices(FOUR_POINT_SCALE, weights=weights, k=1)[0]

        # 評価に関する質問
        for eval_question in EVALUATION_QUESTIONS:
            row[eval_question] = random.choices(FOUR_POINT_SCALE, weights=weights, k=1)[0]

        # 自由記述
        row["この授業に関して意見・感想があれば、ご記入ください。"] = random.choice(FREE_COMMENTS)

        data.append(row)

    return pd.DataFrame(data)


def main():
    """メイン処理：複数科目のサンプルデータを生成"""
    output_dir = Path("sample_data")
    output_dir.mkdir(exist_ok=True)

    print("授業アンケートのサンプルデータを生成しています...")

    # 各科目ごとにデータを生成
    for subject in SUBJECTS:
        # 30〜60件のランダムな回答数
        num_responses = random.randint(30, 60)
        df = generate_sample_data(num_responses=num_responses, subject=subject)

        # ファイル名を作成（日本語を含むがテスト用なので許容）
        filename = output_dir / f"survey_{subject.replace('/', '_')}.csv"

        # CSVとして保存（文字化け防止のためUTF-8 with BOM）
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"  ✓ {filename} ({num_responses}件の回答)")

    # 全科目を混在させたデータも生成
    df_mixed = generate_sample_data(num_responses=100)
    filename_mixed = output_dir / "survey_mixed.csv"
    df_mixed.to_csv(filename_mixed, index=False, encoding="utf-8-sig")
    print(f"  ✓ {filename_mixed} (100件の回答、複数科目混在)")

    print("\nサンプルデータの生成が完了しました！")
    print(f"出力先: {output_dir.absolute()}")


if __name__ == "__main__":
    main()
