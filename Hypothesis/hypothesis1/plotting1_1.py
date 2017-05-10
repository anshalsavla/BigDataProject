import plotly.plotly as py
import plotly
import plotly.graph_objs as go
from plotly.graph_objs import Layout

plotly.tools.set_credentials_file(username='Drumil', api_key='GzRGDhLIvjqOZCGSLHUK')

import pandas as pd


df = pd.read_csv("hyp1.csv")

data = [go.Bar(
          x=df.Date,
          y=df['Total_Complaints'])]

py.plot({"data":data, "layout":Layout(title="Date vs Heating Complaints")})