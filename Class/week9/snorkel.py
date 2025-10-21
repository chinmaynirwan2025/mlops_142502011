from snorkel.labeling import labeling_function, PandasLFApplier
from snorkel.labeling.model.label_model import LabelModel

import pandas as pd
spm , ham , abstain = 1,0,-1
data={
    "email": [
        "Congratulations! You’ve won a free iPhone.",
        "Meeting at 10am tomorrow.",
        "Click here to claim your prize now!",
        "Lunch at the new cafe?",
        "Earn money fast, no work required.",
        "Can you review the report?"
    ]
}
df = pd.DataFrame(data)

#=====Labelling Funcs=============
@labeling_function()
def lf_prize_keywords(x):
    keywords=["prize","won","free","claim"]
    return spm if any(word in x.email.lower() for word in keywords) else abstain

@labeling_function()
def lf_money_keywords(x):
    return spm if "money" in x.email.lower() else abstain

@labeling_function()
def lf_meeting_keywords(x):
    keywords = ["meeting", "report", "review"]
    return ham if any(word in x.email.lower() for word in keywords) else abstain

#==========Applying labeling funcs =============
lfs = [lf_prize_keywords,lf_money_keywords,lf_meeting_keywords]
applier = PandasLFApplier(lfs=lfs)
L_train = applier.apply(df=df)
#==============Train label model =================
label_model = LabelModel(cardinality=2,verbose=False)
label_model.fit(L_train,n_epochs=300,seed=123)

#=========Predict
df["label"]=label_model.predict(L=L_train)
