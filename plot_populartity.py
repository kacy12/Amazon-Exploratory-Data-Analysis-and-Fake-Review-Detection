# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import os
import math

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

def get_file_names(input_directory):
    result_list = []
    for file_name in os.listdir(input_directory):
        if file_name.endswith(".json"):
            result_list.append(file_name)
    return result_list

def get_data(input_directory, file_list):
    outdata = pd.DataFrame()
    for file_name in file_list:
        path = input_directory + "/" + file_name
        data = pd.read_json(path, lines=True)
        outdata=pd.concat([outdata, data], ignore_index=True)
    return outdata

data = pd.read_json("Archive/popularity/all_sales_data.json", orient='split')
data = data.sort_values(by=['count'], ascending=False)
data = data[:20]
category = data['category']
count = data['count']
sale = data['sale']
available_category = category.unique()

year_data = pd.read_json("Archive/popularity/all_sales_yearly_data.json", orient='split')
year_data = year_data.sort_values(by=['year'], ascending=False)

app.layout = html.Div(children=[
    html.H1(children='Amazon Reviews'),

    html.Div(children='''
        Here is popularity_trends_analysis!!!!
    '''),

    html.Div(children='''
        For count top20 category!!!!
    '''),

    html.Div([
        dcc.Graph(
            id='all_brand_count',
            figure={
                'data': [
                    {'x': category, 'y': count, 'type': 'bar', 'name': 'sale'}
                ],
                'layout': {
                    'title': 'Info of top20 category count',
                    'yaxis': {
                        "title": 'count'
                    },
                    'height': 300,
                    'margin': {'l': 50, 'b': 50, 'r': 10, 't': 30},
                }
            }
        )
    ],style={'width': '45%', 'display': 'inline-block', 'padding': '0 20'}),

    html.Div([
        dcc.Graph(
            id='all_category_sale',
            figure={
                'data': [
                    {'x': category, 'y': sale, 'type': 'bar', 'name': 'count'}
                ],
                'layout': {
                    'title': 'Info of top20 category sale',
                    'yaxis': {
                        "title": 'sale'
                    },
                    'height': 300,
                    'margin': {'l': 40, 'b': 50, 'r': 10, 't': 30}
                }
            }
        )
    ],style={'width': '45%', 'display': 'inline-block', 'padding': '0 20'}),

    html.Div([
        dcc.Dropdown(
            id='category',
            options=[{'label': i, 'value': i} for i in category.unique()],
            value='Baby'
        )
    ], style={'width': '30%', 'padding': '0px 20px 20px 20px'}),

    html.Div([
        dcc.Graph(id='category_year_count', hoverData={'points': [{'x': '2003'}]}),
    ], style={'width': '45%', 'display': 'inline-block'}),

    html.Div([
        dcc.Graph(id='category_month_count')
    ],
        style={'width': '40%', 'display': 'inline-block', 'padding': '0 20'}),

    html.Div([
        dcc.Graph(id='category_year_sale', hoverData={'points': [{'x': '2003'}]}),
    ], style={'width': '45%', 'display': 'inline-block'}),

    html.Div([
        dcc.Graph(id='category_month_sale')
    ],
    style={'width': '40%', 'display': 'inline-block', 'padding': '0 20'})


])

@app.callback(
    dash.dependencies.Output('category_year_count', 'figure'),
    [dash.dependencies.Input('category', 'value')])
def update_category_year_count(category):
    df = year_data[year_data['category'] == category]
    count = df['count']
    year = df['year']

    return {
        'data': [
            {'x': year, 'y': count, 'type': 'line', 'name': 'cate_sale'}
        ],
        'layout': {
                'xaxis': {"title": 'year'},
                'yaxis': {"title": 'count'},
                'title': 'Category info of '+ category,
                'height': 250,
                'margin': {'l': 40, 'b': 20, 'r': 10, 't': 10}
         }
    }

@app.callback(
    dash.dependencies.Output('category_month_count', 'figure'),
    [dash.dependencies.Input('category_year_count', 'hoverData')])
def update_category_month_count(hoverData):
    month_data = pd.read_json("Archive/popularity/all_sales_monthly_data.json", orient='split')
    year = hoverData['points'][0]['x']
    month_data = month_data[month_data.year == year]
    month_data = month_data.sort_values(by=['month'], ascending=False)
    month = month_data['month']
    count = month_data['count']

    return {
        'data': [
            {'x': month, 'y': count, 'type': 'bar', 'name': 'cate_month_count'}
        ],
        'layout': {
            'xaxis': {"title": 'month'},
            'yaxis': {"title": 'count'},
            'title': 'sale changing with time',
            'height': 250,
            'margin': {'l': 20, 'b': 20, 'r': 10, 't': 10}
        }
    }

@app.callback(
    dash.dependencies.Output('category_year_sale', 'figure'),
    [dash.dependencies.Input('category', 'value')])
def update_category_year_count(category):
    df = year_data[year_data['category'] == category]
    sale = df['sale']
    year = df['year']

    return {
        'data': [
            {'x': year, 'y': sale, 'type': 'line', 'name': 'cate_sale'}
        ],
        'layout': {
                'xaxis': {"title": 'year'},
                'yaxis': {"title": 'sale'},
                'height': 250,
                'margin': {'l': 40, 'b': 20, 'r': 10, 't': 10}
         }
    }

@app.callback(
    dash.dependencies.Output('category_month_sale', 'figure'),
    [dash.dependencies.Input('category_year_sale', 'hoverData')])
def update_category_month_count(hoverData):
    month_data = pd.read_json("Archive/popularity/all_sales_monthly_data.json", orient='split')
    year = hoverData['points'][0]['x']
    month_data = month_data[month_data.year == year]
    month_data = month_data.sort_values(by=['month'], ascending=False)
    month = month_data['month']
    sale = month_data['sale']

    return {
        'data': [
            {'x': month, 'y': sale, 'type': 'bar', 'name': 'cate_month_sale'}
        ],
        'layout': {
            'xaxis': {"title": 'month'},
            'yaxis': {"title": 'sale'},
            'title': 'sale changing with time',
            'height': 250,
            'margin': {'l': 20, 'b': 20, 'r': 10, 't': 10}
        }
    }


if __name__ == '__main__':
    app.run_server(debug=True, port=8051)