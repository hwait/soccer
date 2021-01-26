import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,f1_score
)

def odds2prob(df):
    df['odds_away']=1/df['odds_away']
    df['odds_draw']=1/df['odds_draw']
    df['odds_home']=1/df['odds_home']
    df['margin']=df[['odds_away','odds_draw','odds_home']].sum(axis=1)
    df['odds_away']=df['odds_away']/df['margin']
    df['odds_draw']=df['odds_draw']/df['margin']
    df['odds_home']=df['odds_home']/df['margin']
    return df[['odds_away','odds_draw','odds_home']]

def get_prevalence(y):
    prevalence=np.mean(y)
    return prevalence

def get_true_pos(y, pred, th=0.5):
    pred_t = (pred > th)
    return np.sum((pred_t == True) & (y == 1))


def get_true_neg(y, pred, th=0.5):
    pred_t = (pred > th)
    return np.sum((pred_t == False) & (y == 0))

def get_accuracy(y, pred, th=0.5):
    TP = get_true_pos(y, pred, th=th)
    TN = get_true_neg(y, pred, th=th)
    FP = get_false_pos(y, pred, th=th)
    FN = get_false_neg(y, pred, th=th)
    accuracy=(TP+TN) / (TP + TN + FP + FN)
    return accuracy

def get_false_neg(y, pred, th=0.5):
    pred_t = (pred > th)
    return np.sum((pred_t == False) & (y == 1))


def get_false_pos(y, pred, th=0.5):
    pred_t = (pred > th)
    return np.sum((pred_t == True) & (y == 0))

def get_sensitivity(y, pred, th=0.5):
    TP = get_true_pos(y, pred, th=th)
    FN = get_false_neg(y, pred, th=th)
    sensitivity=TP / (TP + FN)
    return sensitivity

def get_specificity(y, pred, th=0.5):
    TN = get_true_neg(y, pred, th=th)
    FP = get_false_pos(y, pred, th=th)
    specificity=TN / (TN + FP)
    return specificity

def get_ppv(y, pred, th=0.5):
    TP = get_true_pos(y, pred, th=th)
    FP = get_false_pos(y, pred, th=th)
    PPV=TP / (TP+FP)
    return PPV

def get_npv(y, pred, th=0.5):
    TN = get_true_neg(y, pred, th=th)
    FN = get_false_neg(y, pred, th=th)
    NPV = TN / (TN+FN)
    return NPV

def get_performance_metrics(y, pred, class_labels, tp=get_true_pos,
                            tn=get_true_neg, fp=get_false_pos,
                            fn=get_false_neg,
                            acc=get_accuracy, prevalence=get_prevalence, spec=get_specificity,
                            sens=get_sensitivity, ppv=get_ppv, npv=get_npv, auc=roc_auc_score, f1=f1_score,
                            thresholds=[]):
    if len(thresholds) != len(class_labels):
        thresholds = [.5] * len(class_labels)

    columns = ["Name", "TP", "TN", "FP", "FN", "Accuracy", "Prevalence", "Sensitivity", "Specificity", "PPV", "NPV", "AUC", "F1", "Threshold"]
    res=[]
    
    for i in range(len(class_labels)):
        res.append({
            columns[0] : class_labels[i],
            columns[1] : round(tp(y[:, i], pred[:, i]), 3) if tp != None else "Not Defined",
            columns[2] : round(tn(y[:, i], pred[:, i]), 3) if tn != None else "Not Defined",
            columns[3] : round(fp(y[:, i], pred[:, i]), 3) if fp != None else "Not Defined",
            columns[4] : round(fn(y[:, i], pred[:, i]), 3) if fn != None else "Not Defined",
            columns[5] : round(acc(y[:, i], pred[:, i], thresholds[i]), 3) if acc != None else "Not Defined",
            columns[6] : round(prevalence(y[:, i]), 3) if prevalence != None else "Not Defined",
            columns[7] : round(sens(y[:, i], pred[:, i], thresholds[i]), 3) if sens != None else "Not Defined",
            columns[8] : round(spec(y[:, i], pred[:, i], thresholds[i]), 3) if spec != None else "Not Defined",
            columns[9] : round(ppv(y[:, i], pred[:, i], thresholds[i]), 3) if ppv != None else "Not Defined",
            columns[10] : round(npv(y[:, i], pred[:, i], thresholds[i]), 3) if npv != None else "Not Defined",
            columns[11] : round(auc(y[:, i], pred[:, i]), 3) if auc != None else "Not Defined",
            columns[12] : round(f1(y[:, i], pred[:, i] > thresholds[i]), 3) if f1 != None else "Not Defined",
            columns[13] : round(thresholds[i], 3)
        })
    df = pd.DataFrame(res)
    return df


def print_confidence_intervals(class_labels, statistics):
    df = pd.DataFrame(columns=["Mean AUC (CI 5%-95%)"])
    for i in range(len(class_labels)):
        mean = statistics.mean(axis=1)[i]
        max_ = np.quantile(statistics, .95, axis=1)[i]
        min_ = np.quantile(statistics, .05, axis=1)[i]
        df.loc[class_labels[i]] = ["%.2f (%.2f-%.2f)" % (mean, min_, max_)]
    return df


def get_curve(gt, pred, target_names, curve='roc'):
    for i in range(len(target_names)):
        if curve == 'roc':
            curve_function = roc_curve
            auc_roc = roc_auc_score(gt[:, i], pred[:, i])
            label = target_names[i] + " AUC: %.3f " % auc_roc
            xlabel = "False positive rate"
            ylabel = "True positive rate"
            a, b, _ = curve_function(gt[:, i], pred[:, i])
            plt.figure(1, figsize=(7, 7))
            plt.plot([0, 1], [0, 1], 'k--')
            plt.plot(a, b, label=label)
            plt.xlabel(xlabel)
            plt.ylabel(ylabel)

            plt.legend(loc='upper center', bbox_to_anchor=(1.3, 1),
                       fancybox=True, ncol=1)
        elif curve == 'prc':
            precision, recall, _ = precision_recall_curve(gt[:, i], pred[:, i])
            average_precision = average_precision_score(gt[:, i], pred[:, i])
            label = target_names[i] + " Avg.: %.3f " % average_precision
            plt.figure(1, figsize=(7, 7))
            plt.step(recall, precision, where='post', label=label)
            plt.xlabel('Recall')
            plt.ylabel('Precision')
            plt.ylim([0.0, 1.05])
            plt.xlim([0.0, 1.0])
            plt.legend(loc='upper center', bbox_to_anchor=(1.3, 1),
                       fancybox=True, ncol=1)
