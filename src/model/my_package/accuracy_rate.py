# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30
import pandas as pd

def threshold_accuracy(accuracy, result_proba, threshold):
    """
    accuracy:True/FalseのSeries
    result_proba：ランダムフォレスト実行結果で確率の中の1列
    threshold:しきい値
    """
    # 確率がしきい値を超えているか
    prediction = (result_proba > threshold)
    # しきい値を超えているかで予測した場合に正解と一致する数
    accuracy_count = (accuracy == prediction).sum()
    # 正解率
    accuracy_rate = accuracy_count / len(accuracy)
    if prediction.sum() != 0:
        # 適合率
        precision = (accuracy & prediction).sum() / prediction.sum()
    else:
        precision = None
    
    if accuracy.sum() != 0:
        # 再現率
        recall = (accuracy & prediction).sum() / accuracy.sum()
    else:
        recall = None

    return [accuracy_rate, precision, recall]
#%%
def threshold_accuracy_fvalue(accuracy, result_proba, threshold, equal=False):
    """
    accuracy:True/FalseのSeries
    result_proba：ランダムフォレスト実行結果で確率の中の1列
    threshold:しきい値
    equal:確率がしきい値と等しい場合を含めるか
    """
    # 確率がしきい値を超えているか
    if equal:
        prediction = (result_proba >= threshold)
    else:
        prediction = (result_proba > threshold)
    
    # 正解：True、予測：True
    TP = (prediction & accuracy).sum()
    # 正解：Fasle、予測：True
    FP = prediction.sum() - TP
    # 正解：True、予測：False
    FN = accuracy.sum() - TP
    # 予測：False、正解：False
    TN = (~prediction & ~accuracy).sum()

    # しきい値を超えているかで予測した場合に正解と一致する数
    accuracy_count = (accuracy == prediction).sum()
    # 正解率
    accuracy_rate = accuracy_count / len(accuracy)
    if prediction.sum() != 0:
        # 適合率
        precision = (accuracy & prediction).sum() / prediction.sum()
    else:
        precision = None
    
    if accuracy.sum() != 0:
        # 再現率
        recall = (accuracy & prediction).sum() / accuracy.sum()
    else:
        recall = None
        
    # F値
    if (precision is not None) & (recall is not None):
        f_value = 2 * (precision * recall) / (precision + recall)
    else:
        f_value = None
    
    # 正解Falseの再現率
    an_recall = TN / (FP + TN)
    
    result = [TP, FP, FN, TN,
            accuracy_rate, precision, recall, f_value]
    
    return result

# def all_threshold_accuracy(accuracy_column, predict_column, target):
#
#     accuracy = (accuracy_column == target)
#     result = pd.DataFrame(index=['正解率', '適合率', '再現率'])
#     for thr in range(-1, 11):
#         threshold = thr * 0.1
#         label = str(threshold)[0]
#         if thr == -1:
#             label = '全て' + target + 'と予測'
#         result[label] = threshold_accuracy(accuracy, predict_column, threshold)
#         result.columns.name = 'しきい値'
#     return result.T

#%%
def all_threshold_accuracy_fvalue(accuracy_column, predict_column, target,  thr_list):
    
    accuracy = (accuracy_column == int(target))
    result = pd.DataFrame(index=['TP', 'FP', 'FN', 'TN', '正解率', '適合率', '再現率', 'F値'])

    for thr in thr_list:
        threshold = thr
        label = str(threshold)
        result[label] = threshold_accuracy_fvalue(accuracy, predict_column, threshold)
   
    result.columns.name = 'しきい値'
    return result.T.iloc[::-1,:]