import pickle
import pandas as pd
from kafka import KafkaConsumer
from src.features.build_features import build_features
from pathlib import Path
import json
from elasticsearch import Elasticsearch, helpers
import numpy as np

project_dir = Path(__file__).resolve().parents[2]

def predict_model(df):
   
    X_t = df.drop(['user_id'], axis=1)
    Y_t = df['user_id']
    indexs = np.sort(list(set(Y_t)))
   
    prob =rf.predict_proba(X_t)
    prediction =[]
    known=[]
    unknown =[]

    for i,val in enumerate(prob):
      max_value = max(val)
      index = indexs[np.argmax(val)]
      if(max_value>0.28):
        known.append(index)
        prediction.append(index)
      else:
        unknown.append([max_value, index,i] )
        prediction.append(0)
    predict_unkown = np.array(prediction)

    random_forest_pred_score = predict_unkown
    X_t["predection"] = random_forest_pred_score
    return X_t

elastic_client = Elasticsearch(
    "https://localhost:9200",
    http_auth=("elastic", "2BD1zoltCxhys4lg6GOa"), use_ssl=True, verify_certs=False
)

consumer = KafkaConsumer('ml-keystroke',
                         bootstrap_servers='localhost:9092',
                         auto_offset_reset='earliest')

# Load the model
with open(str(project_dir)+"\\models\\RF.sav", 'rb') as pickle_file:
    rf = pickle.load(pickle_file)
data_new = []
 # Ingesting data from input topic
for i,message in enumerate(consumer):
    if i < 100:

        #df = pd.DataFrame(list(domain_m))
        domain_m = json.loads(message.value.decode("utf-8"))
        data_new.append(domain_m)
        x = pd.DataFrame.from_dict(data_new)
        result = predict_model(x)
        print("result = ",result)
        helpers.bulk(elastic_client, x.transpose().to_dict().values(), index="ml-predictions-domains")
    else:
        break