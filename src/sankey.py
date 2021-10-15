from itertools import cycle
import plotly.graph_objects as go
import plotly.express as px
from plotly import colors
 
def genSankey(df, num_levels=5, viewEnd=True, filter=None, labelsmap=None):

  setAlpha = lambda hex,alpha: f"rgba{tuple( list( colors.hex_to_rgb(hex))+[alpha])}"
  removeLabelPrefix = lambda strg: strg.split('_',maxsplit=1)[1]

  df=df.copy()

  if filter:
    f,n = filter.split('_')
    df = {
      'top': lambda df, n: df.sort_values('count',ascending=False).head(n),
      'bottom': lambda df, n: df.sort_values('count',ascending=True).head(n),
      'greaterthan': lambda df, n: df[df['count']>n],
      'lowerthan': lambda df, n: df[df['count']<n]
    }[f](df, int(n))

  if viewEnd==True:
    for i in df.columns:
      df[i].fillna(f"{i}_END",inplace=True)

  num_threads = df.groupby([0,1]).agg('sum')['count'].sum() # total number of sequences (threads) of user interactions

  num_levels = min(len(df.columns)-1,num_levels) # prevent specifying invalid number of levels
  num_levels = max(num_levels,2) # at least 2 levels 

  def getLinks(df,cols=[0,1]):
    # Métricas: contagem, pct relativa, pct total
    d = df[cols+['count']].groupby(cols).agg(sum)
    d_1 = d.groupby(level=0).apply(lambda x: x/x.sum()).rename(columns={'count':'pct_rel_src'})
    d_2 = d['count'].apply(lambda x: x/d.sum()).rename(columns={'count':'pct_level'})
    j = d.join(d_1).join(d_2).reset_index().to_records(index=False)
    return list(zip(* j ))

    

  levels = []
  for i in range(num_levels-1):
    levels.append( getLinks(df,cols=[i,i+1]) )

  # Unstack lists
  l = []
  for level in levels:
    for i,data in enumerate(level):
      try: l[i] += data
      except: l.append( list(data) )

  s,t,v,pct_rel,pct_level = l

  labels = sorted(set(s+t))


  node_label_map = dict(enumerate(  labels  ))
  node_label_map_inv = { v:k for k,v in node_label_map.items() }

  labels_colors = dict( zip( set( removeLabelPrefix(l) for l in labels), cycle(px.colors.qualitative.Dark24) ))

  getNodeDegree = lambda n: sum( v[i] for i,srcn in enumerate(s) if srcn==n )
  lookup = lambda d,k: d[k] if d.get(k,None) is not None else k
  

  data = dict(
    node = dict(
          pad = 150,
          thickness = 20,
          line = dict(color = "black", width = .5),
          label = [ removeLabelPrefix(l) for l in labels ],
          color = [ labels_colors[removeLabelPrefix(l)] for l in labels ],
          customdata = [ 100*getNodeDegree(l)/num_threads for l in labels ],
          hovertemplate = '%{value} usuários <br />%{customdata:.2f}% dos usuários <br /><extra></extra>'
        ),

    link = dict(
        source = [ node_label_map_inv[i] for i in s ],
        target = [ node_label_map_inv[i] for i in t ],
        value = [  i for i in v ],
        color = [ setAlpha(labels_colors[removeLabelPrefix(node)],0.15) for node in s ],
        customdata = list(zip(* [
                                [100*i for i in pct_level ], # Porcentagem total 
                                [ 100* i for i in pct_rel], # Porcentagem relativa ao src

                              [i/100 for i in v ]
                              ])),

        hovertemplate = 'src: %{source.label} <br />tgt: %{target.label} <br />----<br />'+
                        '%{value} usuários <br />'+
                        '    %{customdata[0]:.1f}% do total<br />'+
                        '    %{customdata[1]:.1f}% da fonte'+
                        '<extra></extra>'

    ),

    other = dict(
        numthreads = num_threads
    )
  )


  # Construindo o diagrama

  fig = go.Figure(data=[go.Sankey(
      valueformat='.0f',
      node = data['node'],
      link = data['link']

      )])
  return fig,data

#fig, data= genSankey(d,num_levels=2,viewEnd=True,filter='greaterthan_10')


if __name__=='__main__':
  from converter import Converter

  fpath='data/Registros de Auditoria-data-30_09_2021 15_59_50.csv'
  c = Converter(fpath)


  fig, data= genSankey(c.to_sequences() ,num_levels=4,viewEnd=True,filter='greaterthan_5')
  fig.update_layout(title_text=f"Interações de {data['other']['numthreads']} usuários no Login. Janela temporal de  segundos.", font_size=10, height=800)
  fig.show()
  