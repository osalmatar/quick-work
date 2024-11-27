import pandas as pd

ATR1 = pd.read_csv("tickers8/ATR.csv")
ATR2 = pd.read_csv("tickers9/ATR.csv")

D_EW_B1 = pd.read_csv("tickers8/D_EW_B.csv")
D_EW_B2 = pd.read_csv("tickers9/D_EW_B.csv")

D_EW_S1 = pd.read_csv("tickers8/D_EW_S.csv")
D_EW_S2 = pd.read_csv("tickers9/D_EW_S.csv")

EW_Conv1 = pd.read_csv("tickers8/EW_Conv.csv")
EW_Conv2 = pd.read_csv("tickers9/EW_Conv.csv")

HHLL1 = pd.read_csv("tickers8/HHLL.csv")
HHLL2 = pd.read_csv("tickers9/HHLL.csv")

LR_Explore1 = pd.read_csv("tickers8/LR_Explore.csv")
LR_Explore2 = pd.read_csv("tickers9/LR_Explore.csv")

Pattern_Revv1 = pd.read_csv("tickers8/Pattern_Revv.csv")
Pattern_Revv2 = pd.read_csv("tickers9/Pattern_Revv.csv")

SCTR_Trial1 = pd.read_csv("tickers8/SCTR_Trial.csv")
SCTR_Trial2 = pd.read_csv("tickers9/SCTR_Trial.csv")

ZigZag1 = pd.read_csv("tickers8/ZigZag.csv")
ZigZag2 = pd.read_csv("tickers9/ZigZag.csv")

datasets = {
    "ATR": (ATR1, ATR2),
    "D_EW_B": (D_EW_B1, D_EW_B2),
    "D_EW_S": (D_EW_S1, D_EW_S2),
    "EW_Conv": (EW_Conv1, EW_Conv2),
    "HHLL": (HHLL1, HHLL2),
    "LR_Explore": (LR_Explore1, LR_Explore2),
    "Pattern_Revv": (Pattern_Revv1, Pattern_Revv2),
    "SCTR_Trial": (SCTR_Trial1, SCTR_Trial2),
    "ZigZag": (ZigZag1, ZigZag2)
}

# Initialize a list for comparison results
comparison_results = []

for name, (df1, df2) in datasets.items():
    types1 = df1.dtypes.to_dict()
    types2 = df2.dtypes.to_dict()
    
    comparison = {
        "File Name": name,
        "tickers8 Columns": list(types1.keys()),
        "tickers9 Columns": list(types2.keys()),
        "tickers8 Types": list(types1.values()),
        "tickers9 Types": list(types2.values()),
        "Match": types1 == types2
    }
    
    comparison_results.append(comparison)

# Display the comparison as a DataFrame
comparison_df = pd.DataFrame(comparison_results)



