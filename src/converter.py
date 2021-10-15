import config
import pandas as pd
import numpy as np

class Converter:

  def __init__(self, fpath):
    self.df=None
    with open (fpath,encoding='utf-8') as f:
      self.df = pd.read_csv(f,dtype={'Conta':str})
    
    # Cleaning
    self.df.dropna(subset=['Conta'],inplace=True) # ignora interações em que o CPF não é capturado
    self.df.fillna(value={'msg':'OK'},inplace=True)
    self.df=self.df[['Data do Evento','Módulo','msg','Recurso','Status','Conta']] # Reduz o n de colunas do dataframe

    # Data enrichment
    try: self.enrich()
    except: pass


  def enrich(self, ignore_irrelevant=True):
    # Adiciona aliases ao dataframe. Ajuda na compreensão das interações entre módulos e mensagens de erro
    df_rec = pd.read_csv( f'https://docs.google.com/spreadsheets/export?format=csv&id={config.google_spreadsheet_id}&gid={config.google_wsht_modules_id}' )
    df_msg = pd.read_csv( f'https://docs.google.com/spreadsheets/export?format=csv&id={config.google_spreadsheet_id}&gid={config.google_wsht_msgs_id}' )

    df_rec.fillna({'Incluir':1},inplace=True)
    df_rec.rename({'Alias':'rec_alias'}, axis=1, inplace=True)
    df_msg.rename({'Alias':'msg_alias'}, axis=1, inplace=True)

    # Enriquecer o dataframe
    self.df = self.df.merge(df_rec[['Módulo','Recurso','rec_alias','Incluir']],on=['Módulo','Recurso'])
    self.df = self.df.merge(df_msg[['Módulo','Recurso','msg','msg_alias']],on=['Módulo','Recurso','msg'],how='left')

    # Ignorar eventos de recursos irrelevantes
    if ignore_irrelevant:
      self.df = self.df[self.df['Incluir']==1]


  def to_sequences(self, key='Conta', elmt='fullmsg',useAlias=True):
    """Tranforma os dados tabulares em sequências"""

    def build_full_msg(rec):
      # Nomeia o evento, contendo todas as informações de módulo, recurso e mensagem
      if useAlias:
        modrec = rec['rec_alias'] if not pd.isnull(rec['rec_alias']) else f"{rec['Módulo']}:{rec['Recurso']}"
        msg = rec['msg_alias'] if not pd.isnull(rec['msg_alias']) else rec['msg']
        return f"{modrec}::{msg}"
      else:
        return f"{x['Módulo']}:{x['Recurso']}::{x['msg']}"


    self.df['fullmsg'] = self.df.apply( build_full_msg, axis=1)

    self.df.fillna(value={elmt:'OK'},inplace=True)
    
    grouped = self.df.groupby(key)
    groupToSeq = lambda g: tuple( g.sort_values(by=key,ascending=True)[elmt].values )

    # formata o dataframe em sequências
    seqs = grouped.apply(groupToSeq)
    df_s = pd.DataFrame.from_records(seqs,index=seqs.index)
    df_s = pd.DataFrame( df_s.groupby(df_s.columns.tolist(),dropna=False).size(), columns=['count'] )
    df_s.reset_index(inplace=True)

    # adiciona prefixo p/ cada nível (menos o último, que é a contagem)
    for i in df_s.columns[:-1]:
      df_s[i] = df_s[i].apply(lambda x: f'{i}_{x}' if not pd.isnull(x) else np.nan)
    
    return df_s

  def show(self):
    print(self.df)


if __name__=='__main__':
  fpath='data/Registros de Auditoria-data-30_09_2021 15_59_50.csv'
  c = Converter(fpath)
  print(c.to_sequences())

  