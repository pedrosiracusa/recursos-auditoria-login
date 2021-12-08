import config
import pandas as pd
import numpy as np

from sankey import gen_sankey_plotly
import sankey

class Converter:

  def __init__(self, fpath=None, **kwargs):
    self.df=None
    self.df_seq = None
    self.timedelta=None
    self.startdatetime=None

    if fpath:
      with open (fpath,encoding='utf-8') as f:
        self.df = pd.read_csv(f,dtype={'Conta':str})
    else:
      self.df=kwargs.get('df')
    
    self.preprocess()

  @classmethod
  def from_combiner(self):
    return


  def preprocess(self):
    # Cleaning
    self.df.dropna(subset=['Conta'],inplace=True) # ignora interações em que o CPF não é capturado
    self.df.fillna(value={'msg':'OK'},inplace=True)
    self.df=self.df[['Data do Evento','Módulo','msg','Recurso','Status','Conta']] # Reduz o n de colunas do dataframe

    # Data enrichment
    try: self.enrich()
    except: pass

    self.timedelta=( pd.to_datetime(self.df['Data do Evento']).max() - pd.to_datetime(self.df['Data do Evento']).min() ).seconds
    self.startdatetime = pd.to_datetime(self.df['Data do Evento']).min()


  def enrich(self, ignore_irrelevant=True):
    # Adiciona aliases ao dataframe. Ajuda na compreensão das interações entre módulos e mensagens de erro. Depende da existência da planilha do dicionário
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


  def to_sequences(self, key='Conta', elmt='fullmsg',useAlias=True, expand_sequences=True):
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

    # formata o dataframe em sequências
    groupToSeq = lambda g: list( g.sort_values(by=key,ascending=True)[elmt].values )
    seqs = grouped.apply(groupToSeq)
    if expand_sequences:
      seqs = seqs.apply( lambda seq: [ piece for event in seq for piece in event.split('::') if piece!='OK' ] )

    df_s = pd.DataFrame.from_records(seqs,index=seqs.index)
    df_s = pd.DataFrame( df_s.groupby(df_s.columns.tolist(),dropna=False).size(), columns=['count'] )
    df_s.reset_index(inplace=True)

    # adiciona prefixo p/ cada nível (menos o último, que é a contagem)
    for i in df_s.columns[:-1]:
      df_s[i] = df_s[i].apply(lambda x: f'{i}_{x}' if not pd.isnull(x) else np.nan)

    self.df_seq = df_s
    
    return df_s

  def show(self):
    print(self.df)

  def toSankey(self, num_levels=5, filter='greaterthan_5', focusNode=None):
    """Retorna objeto gráfico contendo o Sankey Diagram (Plotly)"""

    seq_df = self.to_sequences()
    
    if focusNode:
      focusNode_colnum = int(focusNode.split('_')[0])
      seq_df = seq_df[ seq_df[ focusNode_colnum ]==f"{focusNode}" ]

    sankey_go, plot_data = gen_sankey_plotly(seq_df, num_levels=num_levels, filter=filter)
    return sankey_go, plot_data

  