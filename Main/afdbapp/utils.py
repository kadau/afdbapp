
"""
ceci est la fonction principale pour la recommandation de film
"""
import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

#Predict with tensorflow model
def tf_df(df):
    df_dummy=pd.concat ([pd.get_dummies(df[['RD','Country_type','Sector']]),df[['unique_project_price']]],axis=1)
    df_dummy.rename(columns={'unique_project_price': 'bank_contrib_m_ua'}, inplace=True)
    keras.backend.clear_session()	
    model = keras.models.load_model('my_model.h5')
    predict = model.predict(df_dummy).flatten()
    df_pd=pd.DataFrame({'predicted_nb':predict})
    df_g=pd.concat([df,df_pd],axis=1)
    return df_g

#review index in global_df
def global_df(df):
    df_g=df.groupby(['RD','Country_type','Sector']).bank_contrib_m_ua.agg({'Bank_fin':'sum'})
    df_g['%']=df_g['Bank_fin']/df_g['Bank_fin'].sum()
    df_n=df.groupby(['RD','Country_type','Sector']).SAP_Code.agg({'nb_project':'count'})
    df_g=df_g.join(df_n,how='inner')
    df_g['unique_project_price']=df_g['Bank_fin']/df_g['nb_project']
    return df_g

def sector_df(df):
    sectors=df['Sector'].unique()
    df_c=pd.DataFrame(columns=['RD','Country_type','Sector','Bank_fin','%','nb_project','unique_project_price'])
    for sector in sectors:
        df_sect=df[df['Sector']==sector]
        df_sect=df_sect.reset_index(drop=True)
        df_sect_g=df_sect.groupby(['RD','Country_type','Sector']).bank_contrib_m_ua.agg({'Bank_fin':'sum'})
        df_sect_g['%']=df_sect_g['Bank_fin']/df_sect_g['Bank_fin'].sum()
        #df_sect_g=df_sect_g.reset_index()
        df_sect_n=df_sect.groupby(['RD','Country_type','Sector']).SAP_Code.agg({'nb_project':'count'})
        df_g=df_sect_g.join(df_sect_n,how='inner')
        df_g['unique_project_price']=df_g['Bank_fin']/df_g['nb_project']
        df_g=df_g.reset_index()
        df_c=pd.concat([df_c,df_g],axis=0)
        df_c=df_c.reset_index(drop=True)
    return df_c

def region_df(df):
    regions=df['RD'].unique()
    df_c=pd.DataFrame(columns=['RD','Country_type','Sector','Bank_fin','%','nb_project','unique_project_price'])
    for region in regions:
        df_rd=df[df['RD']==region]
        df_rd=df_rd.reset_index(drop=True)
        df_rd_g=df_rd.groupby(['RD','Country_type','Sector']).bank_contrib_m_ua.agg({'Bank_fin':'sum'})
        df_rd_g['%']=df_rd_g['Bank_fin']/df_rd_g['Bank_fin'].sum()
        #df_rd_g=df_rd_g.reset_index()
        df_rd_n=df_rd.groupby(['RD','Country_type','Sector']).SAP_Code.agg({'nb_project':'count'})
        df_g=df_rd_g.join(df_rd_n,how='inner')
        df_g['unique_project_price']=df_g['Bank_fin']/df_g['nb_project']
        df_g=df_g.reset_index()
        df_c=pd.concat([df_c,df_g],axis=0)
        df_c=df_c.reset_index(drop=True)
    return df_c

def type_df(df):
    types=df['Country_type'].unique()
    df_c=pd.DataFrame(columns=['RD','Country_type','Sector','Bank_fin','%','nb_project','unique_project_price'])
    for type_ in types:
        df_tp=df[df['Country_type']==type_]
        df_tp=df_tp.reset_index(drop=True)
        df_tp_g=df_tp.groupby(['RD','Country_type','Sector']).bank_contrib_m_ua.agg({'Bank_fin':'sum'})
        df_tp_g['%']=df_tp_g['Bank_fin']/df_tp_g['Bank_fin'].sum()
        #df_tp_g=df_tp_g.reset_index()
        df_tp_n=df_tp.groupby(['RD','Country_type','Sector']).SAP_Code.agg({'nb_project':'count'})
        df_g=df_tp_g.join(df_tp_n,how='inner')
        df_g['unique_project_price']=df_g['Bank_fin']/df_g['nb_project']
        df_g=df_g.reset_index()
        df_c=pd.concat([df_c,df_g],axis=0)
        df_c=df_c.reset_index(drop=True)
    return df_c
	
def type_sector_df(df):
    types=df['Country_type'].unique()
    df_c=pd.DataFrame(columns=['RD','Country_type','Sector','Bank_fin','%','nb_project','unique_project_price'])
    for type_ in types:
        df_tp=df[df['Country_type']==type_]
        df_tp=df_tp.reset_index(drop=True)
        sectors=df_tp['Sector'].unique()
        for sector in sectors:
            df_tp_s=df_tp[df_tp['Sector']==sector]
            df_tp_s_g=df_tp_s.groupby(['RD','Country_type','Sector']).bank_contrib_m_ua.agg({'Bank_fin':'sum'})
            df_tp_s_g['%']=df_tp_s_g['Bank_fin']/df_tp_s_g['Bank_fin'].sum()
            #df_tp_s_g=df_tp_s_g.reset_index()
            df_tp_s_n=df_tp_s.groupby(['RD','Country_type','Sector']).SAP_Code.agg({'nb_project':'count'})			
            df_g=df_tp_s_g.join(df_tp_s_n,how='inner')
            df_g['unique_project_price']=df_g['Bank_fin']/df_g['nb_project']
            df_g=df_g.reset_index()
            df_c=pd.concat([df_c,df_g],axis=0)
            df_c=df_c.reset_index(drop=True)
    return df_c

def global_entire_df(file):

    df=pd.read_excel(file)
    df=df[['SAP_Code','RD','Country_type','Sector','bank_contrib_m_ua']]

    df_dummy=pd.concat ([pd.get_dummies(df[['RD','Country_type','Sector']]),df[['bank_contrib_m_ua']]],axis=1)
    keras.backend.clear_session()	
    model = keras.models.load_model('my_model.h5')
    predict = model.predict(df_dummy).flatten()
    df_pd=pd.DataFrame({'f_predicted_nb':predict})
    df=pd.concat([df,df_pd],axis=1)

    df_g=df.groupby(['RD','Country_type','Sector']).bank_contrib_m_ua.agg({'amount_global':'sum'})
    df_f=df.groupby(['RD','Country_type','Sector']).f_predicted_nb.agg({'f_predicted_nb':'sum'})
    df_g=df_g.join(df_f,how='inner')
    df_g=df_g.reset_index()
    return df_g