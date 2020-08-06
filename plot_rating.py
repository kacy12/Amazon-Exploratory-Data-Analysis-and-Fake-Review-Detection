import dash
from dash.dependencies import Input, Output
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import os

input_directory = "Archive/rating/all_avg_rating_2003-01-01_2015-12-31"

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

df = pd.read_json("Archive/rating/all_avg_rating_2003-01-01_2015-12-31.json", orient='split')
df = df.sort_values(by=['avg_rating'], ascending=False)
avg_rating = df['avg_rating']
available_category = df['category'].unique()

year_data = pd.read_json("Archive/rating/all_avg_rating_yearly_2003-01-01_2015-12-31.json", orient='split')
year_data = year_data.sort_values(by=['year'], ascending=False)

correlation_data = pd.read_csv("correlation_data.csv", sep=',')
cor_category = correlation_data['category']
cor_category = cor_category.unique()

app.layout = html.Div(children=[

    html.H1(style={'font-size':'40px','margin-left': '250px', 'margin-right': 'auto', 'margin-top': 'auto'}, children='Amazon Reviews-Review Rating Analysis'),

    html.Div([
        html.H1(style={'font-size': '20px', 'color':'#97A09B', 'margin-left': 'auto', 'margin-right': 'auto', 'margin-top': 'auto'},
                children='Count of TOP10 Rating Category'),
        dash_table.DataTable(
            style_cell = {
                'overflow': 'hidden',
                'textOverflow': 'ellipsis',
                'maxWidth': 0,
            },
            id='rating-datatable',
            columns=[
                {"name": i, "id": i, "deletable": True, "selectable": True} for i in df.columns
            ],
            data=df.to_dict('records'),
            editable=True,
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            column_selectable="single",
            row_selectable="multi",
            row_deletable=True,
            selected_columns=[],
            selected_rows=[],
            page_action="native",
            page_current= 0,
            page_size= 10,
        ),

        dcc.Graph(
            id='all_category_rating',
            figure={
                'data': [
                    {'x': available_category, 'y': avg_rating, 'type': 'bar', 'name': 'category_rating'}
                ],
                'layout': {
                    'title': 'All category reviews rating',
                    'xaxis': {"title": 'category'},
                    'yaxis': {"title": 'avg_rating'},
                    'height': 300,
                    'margin': {'l': 40, 'b': 70, 'r': 10, 't': 50}
                }
            }
        )
    ],style={'width': '60%', 'padding': '0 20','margin-left': '250px', 'margin-right': '250px', 'margin-top': '50px'}),

    html.Div([
        html.H1(style={'font-size': '20px', 'color': '#97A09B', 'margin-left': 'auto', 'margin-right': 'auto',
                       'margin-top': 'auto'},
                children='Category Rating Changing with Times'),
        dcc.Dropdown(
            id='category',
            options=[{'label': i, 'value': i} for i in available_category],
            value='Baby'
        ),
    ], style={'width': '45%', 'padding': '0 20'}),

    html.Div([
        dcc.Graph(id='category_year_rating', hoverData={'points': [{'x': '2003'}]}),
    ], style={'width': '45%', 'display': 'inline-block'}),

    html.Div([
        dcc.Graph(id='category_month_rating')
    ],style={'width': '45%', 'display': 'inline-block', 'padding': '0 20'}),

    html.Div([
        html.H1(style={'font-size': '20px', 'color': '#97A09B', 'margin-left': 'auto', 'margin-right': 'auto',
                       'margin-top': 'auto'},
                children='Correlation between Categories'),
        dcc.Dropdown(
            id='cor_category',
            options=[{'label': i, 'value': i} for i in cor_category],
            value='Cell_Phones_and_Accessories'
        ),
        dcc.Graph(id='correlation_graph')
    ], style={'width': '60%', 'padding': '0 20', 'margin-left': 'auto', 'margin-right': 'auto'})



])


@app.callback(
    dash.dependencies.Output('category_year_rating', 'figure'),
    [dash.dependencies.Input('category', 'value')])
def update_category_year_rating(category):
    df = year_data[year_data['category'] == category]
    avg_rating = df['avg_rating']
    year = df['year']

    return {
        'data': [
            {'x': year, 'y': avg_rating, 'type': 'line', 'name': 'cate_rating','marker': {'color':'3B0D82'}}
        ],
        'layout': {
                'xaxis': {"title": 'year'},
                'yaxis': {"title": 'avg_rating'},
                'height': 400,
                'margin': {'l': 40, 'b': 70, 'r': 10, 't': 10}
         }
    }

@app.callback(
    dash.dependencies.Output('category_month_rating', 'figure'),
    [dash.dependencies.Input('category_year_rating', 'hoverData'),
     dash.dependencies.Input('category', 'value')])
def update_category_month_rating(hoverData, category):
    month_data = pd.read_json("Archive/rating/all_avg_rating_monthly_2003-01-01_2015-12-31.json", orient='split')
    year = hoverData['points'][0]['x']
    month_data = month_data[month_data.year == year]
    month_data = month_data[month_data['category'] == category]
    month_data = month_data.sort_values(by=['month'])
    month = month_data['month']
    avg_rating = month_data['avg_rating']

    return {
        'data': [
            {'x': month, 'y': avg_rating, 'type': 'line', 'name': 'cate_month_ratng','marker': {'color':'7324D8'}}
        ],
        'layout': {
            'xaxis': {"title": 'month'},
            'yaxis': {"title": 'avg_rating'},
            'height': 400,
            'margin': {'l': 50, 'b': 70, 'r': 10, 't': 10}
        }
    }


@app.callback(
    dash.dependencies.Output('correlation_graph', 'figure'),
    [dash.dependencies.Input('cor_category', 'value')])
def update_all_brand_count(category):
    other_category = correlation_data['category']
    Correlation = correlation_data[category]

    return {
        'data': [dict(x=other_category, y=Correlation, type='line', name='cate_count',marker={'color':'7F0B68'})],
        'layout': {
                'title': 'Correlation with others of '+ category,
                'xaxis': {"title": 'category'},
                'yaxis': {"title": 'correlation'},
                'height': 500,
                'margin': {'l': 50, 'b': 50, 'r': 50, 't': 70}
         }
    }


if __name__ == '__main__':
    app.run_server(debug=True)