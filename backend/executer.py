import pandas as pd

from backend.database import get_engine
def run_query(sql):
    engine=get_engine()
    df=pd.read_sql(sql,engine)
    df=df.replace({float("nan"): None})
    df=df.where(pd.notnull(df), None)   
    df=df.applymap(lambda x: x.item() if hasattr(x, 'item') else x  )
    return df.to_dict(orient='records')


