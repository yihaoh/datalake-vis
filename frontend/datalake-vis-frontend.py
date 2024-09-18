#######################
# Import libraries
import streamlit as st
import plotly.graph_objects as go
import json
import os
import time


#######################
# helper functions
def load_json_files_from_directory(directory_path):
    json_objects = []

    # Loop through all files in the directory
    for filename in os.listdir(directory_path):
        # Check if the file is a JSON file
        if filename.endswith(".json"):
            file_path = os.path.join(directory_path, filename)

            # Open and load the JSON file
            with open(file_path, 'r') as json_file:
                try:
                    data = json.load(json_file)
                    json_objects.append(data)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from file {filename}: {e}")
    sorted_json_objects = sorted(json_objects, key=lambda x: x['query_table']['name'])
    return sorted_json_objects

def get_json_filenames_without_extension(directory_path):
    json_filenames = []

    # Loop through all files in the directory
    for filename in os.listdir(directory_path):
        # Check if the file is a JSON file
        if filename.endswith(".json"):
            # Remove the '.json' extension from the filename
            json_filenames.append(os.path.splitext(filename)[0])
    json_filenames.sort()
    return json_filenames

#######################
# Page configuration
st.set_page_config(
    page_title="Data Lake Visualization",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded")

# alt.themes.enable("dark")

#######################
# CSS styling
st.markdown("""
<style>
[data-testid="column"] {
    overflow-y: scroll;
    height: 100vh;
}

[data-testid="block-container"] {
    padding-left: 2rem;
    padding-right: 2rem;
    padding-top: 1rem;
    padding-bottom: 0rem;
    margin-bottom: -7rem;
}

[data-testid="stVerticalBlock"] {
    padding-left: 0rem;
    padding-right: 0rem;
}


</style>
""", unsafe_allow_html=True)


#######################
# Load data
all_queries = get_json_filenames_without_extension('frontend-data')


#######################
# Sidebar
# if 'query_data' not in st.session_state:
#     st.session_state.query_data = None


def get_query_data(query_table_name):
    # st.session_state.pop('query_data')
    # st.session_state.pop('top_k_plan')
    # time.sleep(5)
    st.session_state.query_data = json.load(open(f'frontend-data/{query_table_name}.json'))
    st.session_state.top_k_plans = st.session_state.query_data['plans']

with st.sidebar:
    st.title('📊 Data Lake Visualization Dashboard')
        
    selected_query = st.selectbox('Select a query table', all_queries)
    # if st.button("Submit", on_click=get_query_data(selected_query)):
    if st.button("Submit", key='submit'):
        
        st.session_state.query_data = json.load(open(f'frontend-data/{selected_query}.json'))
        st.session_state.top_k_plans = st.session_state.query_data['plans']
        st.rerun()

    if 'query_data' in st.session_state:
        st.write('''Query Table''')
        with st.expander(st.session_state.query_data['query_table']['name']):
            for i in [f'{name}: {ty}' for name, ty in zip(st.session_state.query_data['query_table']['column_names'], st.session_state.query_data['query_table']['column_types'])]:
                st.markdown("- " + i)

        st.write('''Result Tables''')
        for rt in st.session_state.query_data['result_tables']:
            with st.expander(rt['name']):
                for i in [f'{name}: {ty}' for name, ty in zip(rt['column_names'], rt['column_types'])]:
                    st.markdown("- " + i)


#######################
# Dashboard Main Panel

# ========== tmp functions

def generate_grouped_bar_chart_preview(categories, data, h=100):
    # Create the bar chart with two groups
    fig = go.Figure(data=[
        go.Bar(name=k, x=categories, y=v)
        for k, v in data.items()
    ])
    
    # Update the layout to group bars
    # fig.update_layout(barmode='group', title_text=f'Grouped Bar Chart {idx + 1}', height=h)
    fig.update_layout(barmode='group', margin=dict(l=1, r=1, t=1, b=1), showlegend=False, height=h)
    
    return fig

def generate_grouped_bar_chart_main(categories, data, h=300):
    # Create the bar chart with two groups
    fig = go.Figure(data=[
        go.Bar(name=k, x=categories, y=v)
        for k, v in data.items()
    ])
    
    # Update the layout to group bars
    # fig.update_layout(barmode='group', title_text=f'Grouped Bar Chart {idx + 1}', height=h)
    fig.update_layout(barmode='group', height=h)
    
    return fig

# ==============================

# Initialize session state to store the clicked chart figure
if 'clicked_chart_index' not in st.session_state:
    st.session_state.clicked_chart_index = None

col = st.columns((1.5, 4.5), gap='small')

with col[0]:
    with st.container(border=True):
        if 'query_data' in st.session_state and st.session_state.query_data:
            st.markdown(f"### Top {st.session_state.query_data['k']} Recommendations")
            
            for i, plan in enumerate(st.session_state.top_k_plans):
                # Button for selecting the chart
                if st.button(f"Show Chart: {plan['title']}", key=f"chart_{i}"):
                    st.session_state.clicked_chart_index = i

                # Display the chart
                fig = generate_grouped_bar_chart_preview(plan['categories'], plan['plot_data'])
                st.plotly_chart(fig, use_container_width=True)


with col[1]:
    # these are for formal demo
    # with st.container(border=True):
    #     st.markdown("## Data Exploration")
    #     # nested_col = st.columns((1,1), gap='small')
        
    #     selected_x_axis = st.selectbox('Select x column', ['a','b','c'])
    #     selected_y_axis = st.selectbox('Select y column', ['a','b','c'])
    #     selected_agg = st.selectbox('Select aggregate function', ['COUNT','SUM','AVG','MIN','MAX'])
    #     st.button("Visualize")

    if st.session_state.clicked_chart_index != None:
        st.markdown(f"## {st.session_state.top_k_plans[st.session_state.clicked_chart_index]['title']}")
    if st.session_state.clicked_chart_index is not None:
        # st.plotly_chart(generate_random_grouped_bar_chart(st.session_state.chart_data_categories, st.session_state.chart_data[st.session_state.clicked_chart_index] ,st.session_state.clicked_chart_index, 600))
        st.plotly_chart(generate_grouped_bar_chart_main(st.session_state.top_k_plans[st.session_state.clicked_chart_index]['categories'], st.session_state.top_k_plans[st.session_state.clicked_chart_index]['plot_data'], 600))
        with st.container(border=True):
            st.markdown("## Series Breakdown")
            for k, v in st.session_state.top_k_plans[st.session_state.clicked_chart_index]['series_info'].items():
                tp_res = k + ":"
                for match in v:
                    if match[0] == 0:   # query table
                        tp_res = f"{tp_res} ({st.session_state.query_data['query_table']['name'], st.session_state.query_data['query_table']['column_names'][match[1]], st.session_state.query_data['query_table']['column_types'][match[1]]})"
                    else:   # result table
                        tp_res = f"{tp_res} ({st.session_state.query_data['result_tables'][match[0]-1]['name'], st.session_state.query_data['result_tables'][match[0]-1]['column_names'][match[1]], st.session_state.query_data['result_tables'][match[0]-1]['column_types'][match[1]]})"
                st.markdown("- " + tp_res)
    else:
        st.write("No chart clicked yet.")

        
