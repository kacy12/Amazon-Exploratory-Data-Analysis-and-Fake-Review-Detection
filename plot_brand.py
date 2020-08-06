# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import os
import math
import numpy as np

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

input_directory = "Archive/brand/all_brand_sale_2003-01-01_2015-12-31"
data_df = get_data(input_directory, get_file_names(input_directory))
available_year = data_df['year'].unique()
available_year = np.sort(available_year)
available_year = np.append(available_year, 'All_year')

all_brand_data = pd.read_json("Archive/brand/all_category_brand_vs_no_brand.json", orient='split')
category_brand = all_brand_data['category']
available_category = category_brand.unique()
count_with_brand = all_brand_data['count_with_brand']
price_with_brand = all_brand_data['price_with_brand']
count_without_brand = all_brand_data['count_without_brand']
price_without_brand = all_brand_data['price_without_brand']


app.layout = html.Div(children=[
    html.H1(style={'font-size': '40px', 'margin-left': '250px', 'margin-right': 'auto', 'margin-top': 'auto'},
            children='Amazon Reviews-Brand Effect Analysis'),

    html.H1(style={'font-size': '20px', 'color': '#97A09B', 'margin-left': 'auto', 'margin-right': 'auto',
                   'margin-top': 'auto'},
            children='Count&Price Comparision with/without brand'),

    html.Div([
        dcc.Graph(
            id='all_category_count_brand_vs_no_brand',
            figure={
                'data': [
                    {'x': category_brand, 'y': count_with_brand, 'type': 'bar', 'name': 'count_with_brand'},
                    {'x': category_brand, 'y': count_without_brand, 'type': 'bar', 'name': 'count_without_brand'}
                ],
                'layout': {
                    'title': 'count with brand vs without',
                    'xaxis': {"title": 'category'},
                    'yaxis': {"title": 'count'},
                    'height': 350,
                    'margin': {'l': 40, 'b': 50, 'r': 10, 't': 30}
                }
            }
        )
    ], style={'width': '50%', 'display': 'inline-block', 'padding': '0 20'}),

    html.Div([
        dcc.Graph(
            id='all_category_price_brand_vs_no_brand',
            figure={
                'data': [
                    {'x': category_brand, 'y': price_with_brand, 'type': 'bar', 'name': 'price_with_brand'},
                    {'x': category_brand, 'y': price_without_brand, 'type': 'bar', 'name': 'price_without_brand'}
                ],
                'layout': {
                    'title': 'price with brand vs without',
                    'xaxis': {"title": 'category'},
                    'yaxis': {"title": 'price'},
                    'height': 350,
                    'margin': {'l': 40, 'b': 50, 'r': 10, 't': 30}
                }
            }
        )
    ], style={'width': '50%', 'display': 'inline-block', 'padding': '0 20'}),

    html.H1(style={'font-size': '20px', 'color': '#97A09B', 'margin-left': 'auto', 'margin-right': 'auto',
                   'margin-top': 'auto'},
            children='Brand Data of All Category with Time'),

    html.Div([
        dcc.Dropdown(
            id='year_dropdown',
            options=[{'label': i, 'value': i} for i in available_year],
            value='All_year'
        )
    ], style={'width': '40%', 'padding': '0 20'}),

    html.Div([
         dcc.Graph(id='all_brand_count'),
         dcc.Graph(id='all_brand_sale'),
         dcc.Graph(id='all_brand_price'),
    ], style={'width': '55%', 'display': 'inline-block', 'padding': '0 20'}),

    html.Div([
        dcc.Graph(id='all_count_price')
    ], style={'width': '40%', 'display': 'inline-block', 'padding': '0 20'}),

    html.H1(style={'font-size': '20px', 'color': '#97A09B', 'margin-left': 'auto', 'margin-right': 'auto',
                   'margin-top': 'auto'},
            children='Brand Data of Each Category with Time'),

    html.Div([
        dcc.Dropdown(
            id='year_dropdown_2',
            options=[{'label': i, 'value': i} for i in available_year],
            value='All_year'
        ),
        dcc.Dropdown(
            id='category_2',
            options=[{'label': i, 'value': i} for i in available_category],
            value='Electronics'
        )
    ], style={'width': '55%', 'padding': '0 20'}),


    html.Div([
        dcc.Graph(id='category_brand_count', hoverData={'points': [{'x': 'Apple'}]}),
        dcc.Graph(id='category_brand_sale'),
        dcc.Graph(id='category_brand_price')
    ], style={'width': '55%', 'display': 'inline-block'}),

    html.Div([
        dcc.Graph(id='brand_time_count'),
        dcc.Graph(id='brand_time_sale')
    ],
    style={'width': '40%', 'display': 'inline-block', 'padding': '0 20'})


])

@app.callback(
    dash.dependencies.Output('all_brand_count', 'figure'),
    [dash.dependencies.Input('year_dropdown', 'value')])
def update_all_brand_count(year):
    if year == 'All_year':
        data = data_df.groupby(['brand'])['count'].sum().reset_index()
    else:
        data = data_df[data_df['year'] == int(year)]

    data = data.sort_values(by=['count'], ascending=False)
    data = data[1:21]
    brand = data['brand']
    count = data['count']
    return {
        'data': [dict(x=brand, y=count, type='bar', name='cate_count',marker={'color':'1C9CE5'})],
        'layout': {
                'title': 'Top20 Brand Info of All Category',
                'yaxis': {"title": 'count'},
                'height': 200,
                'margin': {'l': 30, 'b': 50, 'r': 20, 't': 30}
         }
    }

@app.callback(
    dash.dependencies.Output('all_brand_sale', 'figure'),
    [dash.dependencies.Input('year_dropdown', 'value')])
def update_all_brand_sale(year):
    if year == 'All_year':
        data = data_df.groupby(['brand'])['sale','count'].sum().reset_index()
    else:
        data = data_df[data_df['year'] == int(year)]

    data = data.sort_values(by=['count'], ascending=False)
    data = data[1:21]
    brand = data['brand']
    sale = data['sale']
    return {
        'data': [dict(x=brand, y=sale, type='bar', name='cate_sale', marker={'color':'0F7FBF'})],
        'layout': {
                'yaxis': {"title": 'sale'},
                'height': 200,
                'margin': {'l': 30, 'b': 30, 'r': 10, 't': 10}
         }
    }

@app.callback(
    dash.dependencies.Output('all_brand_price', 'figure'),
    [dash.dependencies.Input('year_dropdown', 'value')])
def update_all_brand_price(year):
    if year == 'All_year':
        data = data_df.groupby(['brand'])['avg_price','count'].sum().reset_index()
    else:
        data = data_df[data_df['year'] == int(year)]

    data = data.sort_values(by=['count'], ascending=False)
    data = data[1:21]
    brand = data['brand']
    avg_price = data['avg_price']
    return {
        'data': [dict(x=brand, y=avg_price, type='bar', name='cate_avg_price',marker={'color':'074993'})],
        'layout': {
                'yaxis': {"title": 'avg_price'},
                'height': 200,
                'margin': {'l': 30, 'b': 30, 'r': 10, 't': 10}
         }
    }

@app.callback(
    dash.dependencies.Output('all_count_price', 'figure'),
    [dash.dependencies.Input('year_dropdown', 'value')])
def update_all_count_price(year):
    if year == 'All_year':
        data = data_df.groupby(['brand'])['avg_price','count'].sum().reset_index()
    else:
        data = data_df[data_df['year'] == int(year)]

    data = data.sort_values(by=['count'], ascending=False)
    count = data['count']
    avg_price = data['avg_price']
    return {
        'data': [dict(x=count, y=avg_price, type='line', name='cate_avg_price',marker={'color':'1E34C2'})],
        'layout': {
                'title': 'Relationship between Count & Price of '+year,
                'xaxis': {"title": 'count'},
                'yaxis': {"title": 'avg_price'},
                'height': 600,
                'margin': {'l': 50, 'b': 30, 'r': 10, 't': 40}
         }
    }


@app.callback(
    dash.dependencies.Output('category_brand_count', 'figure'),
    [dash.dependencies.Input('category_2', 'value'),
     dash.dependencies.Input('year_dropdown_2', 'value')])
def update_brand_count(category, year):
    category_data = pd.read_json("Archive/brand/" + category + "_brand_sale_2003-01-01_2015-12-31_sale_data.json", orient='split')
    if year == 'All_year':
        data = category_data.groupby(['brand'])['count'].sum().reset_index()
    else:
        data = category_data[category_data['year'] == int(year)]

    data = data.sort_values(by=['count'], ascending=False)
    data = data[1:21]
    cate_brand = data['brand']
    cate_count = data['count']
    return {
        'data': [dict(
            x=cate_brand, y=cate_count,type='bar',name='cate_count',marker={'color':'E5921A'})],
        'layout': {
                'yaxis': {"title": 'count'},
                'title': 'Brand info of '+ category,
                'height': 200,
                'margin': {'l': 20, 'b': 50, 'r': 10, 't': 50}
         }
    }

@app.callback(
    dash.dependencies.Output('category_brand_sale', 'figure'),
    [dash.dependencies.Input('category_2', 'value'),
     dash.dependencies.Input('year_dropdown_2', 'value')])
def update_category_brand_sale(category, year):
    category_data = pd.read_json("Archive/brand/" + category + "_brand_sale_2003-01-01_2015-12-31_sale_data.json", orient='split')
    if year == 'All_year':
        data = category_data.groupby(['brand'])['sale','count'].sum().reset_index()
    else:
        data = category_data[category_data['year'] == int(year)]

    data = data.sort_values(by=['count'], ascending=False)
    data = data[1:21]
    cate_brand = data['brand']
    cate_sale = data['sale']
    return {
        'data': [dict(
            x=cate_brand, y=cate_sale,type='bar',name='cate_sale',marker={'color':'BD760F'})],
        'layout': {
                'yaxis': {"title": 'sale'},
                'height': 200,
                'margin': {'l': 20, 'b': 50, 'r': 10, 't': 30}
         }
    }

@app.callback(
    dash.dependencies.Output('category_brand_price', 'figure'),
    [dash.dependencies.Input('category_2', 'value'),
     dash.dependencies.Input('year_dropdown_2', 'value')])
def update_brand_count(category, year):
    category_data = pd.read_json("Archive/brand/"+category+"_brand_sale_2003-01-01_2015-12-31_sale_data.json", orient='split')
    if year == 'All_year':
        data = category_data.groupby(['brand'])['avg_price','count'].sum().reset_index()
    else:
        data = category_data[category_data['year'] == int(year)]

    data = data.sort_values(by=['count'], ascending=False)
    data = data[1:21]
    cate_brand = data['brand']
    cate_price = data['avg_price']
    return {
        'data': [dict(
            x=cate_brand, y=cate_price,type='bar',name='cate_price',marker={'color':'815009'})],
        'layout': {
                'yaxis': {"title": 'avg_price'},
                'height': 200,
                'margin': {'l': 20, 'b': 50, 'r': 10, 't': 30}
         }
    }

@app.callback(
    dash.dependencies.Output('brand_time_count', 'figure'),
    [dash.dependencies.Input('category_2', 'value'),
    dash.dependencies.Input('year_dropdown_2', 'value'),
     dash.dependencies.Input('category_brand_count', 'hoverData')])
def update_sake_timeseries(category, year,hoverData):
    brand_data = pd.read_json("Archive/brand/"+category+"_brandMonth_sale_2003-01-01_2015-12-31_sale_data.json", orient='split')
    brand_name = hoverData['points'][0]['x']
    brand_data = brand_data[brand_data.brand == brand_name]
    if year == 'All_year':
        brand_data = brand_data.groupby(['year'])['count'].sum().reset_index()
    else:
        brand_data = brand_data[brand_data['year'] == int(year)]

    b_count = brand_data['count']
    if year == 'All_year':
        b_time = brand_data['year']
    else:
        b_time = brand_data['month']

    return {
        'data': [
            {'x': b_time, 'y': b_count, 'type': 'line', 'name': 'cate_sale','marker':{'color':'D9560B'}}
        ],
        'layout': {
            'xaxis': {"title": 'time of year/month'},
            'yaxis': {"title": 'count'},
            'title': 'Count Changing of '+ brand_name+' in '+ year,
            'height': 300,
            'margin': {'l': 30, 'b': 50, 'r': 10, 't': 50}
        }
    }

@app.callback(
    dash.dependencies.Output('brand_time_sale', 'figure'),
    [dash.dependencies.Input('category_2', 'value'),
    dash.dependencies.Input('year_dropdown_2', 'value'),
     dash.dependencies.Input('category_brand_count', 'hoverData')])
def update_sake_timeseries(category, year,hoverData):
    brand_data = pd.read_json("Archive/brand/"+category+"_brandMonth_sale_2003-01-01_2015-12-31_sale_data.json", orient='split')
    brand_name = hoverData['points'][0]['x']
    brand_data = brand_data[brand_data.brand == brand_name]
    if year == 'All_year':
        brand_data = brand_data.groupby(['year'])['sale'].sum().reset_index()
    else:
        brand_data = brand_data[brand_data['year'] == int(year)]

    b_sale = brand_data['sale']
    if year == 'All_year':
        b_time = brand_data['year']
    else:
        b_time = brand_data['month']

    return {
        'data': [
            {'x': b_time, 'y': b_sale, 'type': 'line', 'name': 'cate_sale','marker':{'color':'C84F0A'}}
        ],
        'layout': {
            'xaxis': {"title": 'time of year/month'},
            'yaxis': {"title": 'count'},
            'title': 'Sale Changing of '+ brand_name+' in '+ year,
            'height': 300,
            'margin': {'l': 30, 'b': 50, 'r': 10, 't': 30}
        }
    }


if __name__ == '__main__':
    app.run_server(debug=True,port=8052)