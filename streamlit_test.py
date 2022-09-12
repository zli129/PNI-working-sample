import streamlit as st
import numpy as np
import pandas as pd
from scipy import stats
import pymysql
import datetime
import altair as alt

def load_data(start_date = False, end_date = False, headset_num = None):
    
    # Get all data
    # Connect to the database
    db = pymysql.connect(host='ls-4bfee4a8a15c8b219eeb812dcce060d4b3e21e32.ciq1dnc5vjv1.us-east-1.rds.amazonaws.com',
                             user='dbmasteruser',
                             password='pni12345',
                             database='dbmaster')
    
    # Create a cursor to execute the SQL query 
    cursor = db.cursor()
    if not headset_num:
        cursor.execute("select C2.date,C2.headset_num,C2.location,C2.VR_version,C2.tense_relaxed_pre, C2.tense_relaxed_post,C2.tense_relaxed_post-C2.tense_relaxed_pre as 'tense_relaxed_change', C2.anxious_calm_pre,C2.anxious_calm_post,C2.anxious_calm_post-C2.anxious_calm_pre as 'anxious_calm_change',C2.dispirited_empowered_pre,C2.dispirited_empowered_post,C2.dispirited_empowered_post-C2.dispirited_empowered_pre as 'dispirited_empowered_change',count(C1.headset_num) as 'number of times used' from created_data_test_upload C1, created_data_test_upload C2 where C1.date <= C2.date and C1.headset_num = C2.headset_num group by C2.date,C2.headset_num,C2.location,C2.VR_version,C2.tense_relaxed_pre, C2.tense_relaxed_post,C2.tense_relaxed_post-C2.tense_relaxed_pre,C2.anxious_calm_pre,C2.anxious_calm_post, C2.anxious_calm_post-C2.anxious_calm_pre,C2.dispirited_empowered_pre,C2.dispirited_empowered_post,C2.dispirited_empowered_post-C2.dispirited_empowered_pre")
    else:
        cursor.execute("select C2.date,C2.headset_num,C2.location,C2.VR_version,C2.tense_relaxed_pre, C2.tense_relaxed_post,C2.tense_relaxed_post-C2.tense_relaxed_pre as 'tense_relaxed_change', C2.anxious_calm_pre, C2.anxious_calm_post, C2.anxious_calm_post-C2.anxious_calm_pre as 'anxious_calm_change', C2.dispirited_empowered_pre, C2.dispirited_empowered_post, C2.dispirited_empowered_post-C2.dispirited_empowered_pre as 'dispirited_empowered_change', count(C1.headset_num) as 'number of times used' from created_data_test_upload C1, created_data_test_upload C2 where C1.date <= C2.date and C1.headset_num = C2.headset_num and C2.headset_num in {headset} group by C2.date,C2.headset_num,C2.location,C2.VR_version,C2.tense_relaxed_pre, C2.tense_relaxed_post,C2.tense_relaxed_post-C2.tense_relaxed_pre,C2.anxious_calm_pre,C2.anxious_calm_post, C2.anxious_calm_post-C2.anxious_calm_pre,C2.dispirited_empowered_pre,C2.dispirited_empowered_post,C2.dispirited_empowered_post-C2.dispirited_empowered_pre".format(headset=headset_num))
    
    result = cursor.fetchall()

    # Transform the data we get to the pandas Dataframe format
    data_test = pd.DataFrame(result,columns=['date', 'headset_num', 'location', 'VR_version', 'tense_relaxed_pre',
           'tense_relaxed_post','tense_relaxed_change', 'anxious_calm_pre','anxious_calm_post', 'anxious_calm_change',
           'dispirited_empowered_pre', 'dispirited_empowered_post','dispirited_empowered_change', 'number_of_times_used'])

    # Get the data from start_date to end_date
    if start_date and end_date:
        data_test = data_test[(data_test.date > datetime.date(int(start_date.split("-")[0]),int(start_date.split("-")[1]),int(start_date.split("-")[2]))) 
                 & (data_test.date < datetime.date(int(end_date.split("-")[0]),int(end_date.split("-")[1]),int(end_date.split("-")[2])))]
    elif start_date:
        data_test = data_test[data_test.date > datetime.date(int(start_date.split("-")[0]),int(start_date.split("-")[1]),int(start_date.split("-")[2]))]
    elif end_date:
        data_test = data_test[data_test.date < datetime.date(int(end_date.split("-")[0]),int(end_date.split("-")[1]),int(end_date.split("-")[2]))]
    
    return data_test


class report_generator:
    '''
    For report_generator, the only parameter we need to pass is the data. 
    Especially, we need to input the data as the Dataframe format which is a format in python library pandas.
    (We can check how to get the data from MySQL server and transform the data into Dataframe format in the document “MySQL Database Document”).

    Parameters:
        data: Need to be DataFrame. The columns of the Dataframe should be:
             ['date','headset_num', 'location', 'VR_version', 'tense_relaxed_pre',
               'tense_relaxed_post', 'tense_relaxed_change', 'anxious_calm_pre',
               'anxious_calm_post', 'anxious_calm_change', 'dispirited_empowered_pre',
               'dispirited_empowered_post', 'dispirited_empowered_change','number_of_times_used'].
    '''
    
    def __init__(self, data):
        # Create columns: month, day, year
        data['month'] = [data.date.iloc[i].month for i in range(len(data.date))]
        data['day'] = [data.date.iloc[i].day for i in range(len(data.date))]
        data['year'] = [data.date.iloc[i].year for i in range(len(data.date))]
        
        self.data = data

    def missing_checking(self, column_name):
        '''
        # True for "there is missing value"
        # False for "there is no missing value"
        '''
        
        if (True in self.data[column_name].isna().values):
            return True
        return False
        
    def significant_test(self, tense_threshold=0.05, anxious_threshold=0.05, dispirited_threshold=0.05):
        '''
        It will do the t-test automatically to check whether there is a significant improvement after using our application.

        report_generator.significant_test( tense_threshold=0.05, anxious_threshold=0.05, dispirited_threshold=0.05)
        
        Parameters:
        
            tense_threshold: The level of probability (alpha level, level of significance, p) we are going to use in the t-test for tense_relaxed experiment. The default setting is 0.05, which indicates a 5% risk of concluding that a difference exists when there is 	no actual difference.
            
            anxious_threshold: The level of probability (alpha level, level of significance, p) we are going to use in the t-test for anxious_calm experiment. The default setting is 0.05, which indicates a 5% risk of concluding that a difference exists when there is no actual difference.
            
            dispirited_threshold: The level of probability (alpha level, level of significance, p) we are going to use in the t-test for dispirited_empowered experiment. The default setting is 0.05, which indicates a 5% risk of concluding that a difference exists when there is no actual difference.

        Here is an example:
            # Use the default setting
            report_generator.significant_test()

        '''
        
        tense_t_value, tense_p_value = stats.ttest_rel(self.data['tense_relaxed_pre'], self.data['tense_relaxed_post'],alternative='less')
        anxious_t_value, anxious_p_value = stats.ttest_rel(self.data['anxious_calm_pre'], self.data['anxious_calm_post'],alternative='less')
        dispirited_t_value, dispirited_p_value = stats.ttest_rel(self.data['dispirited_empowered_pre'], self.data['dispirited_empowered_post'],alternative='less')
        
        significant_test_result = pd.DataFrame({'t-value':[round(tense_t_value,2),round(anxious_t_value,2),round(dispirited_t_value,2)],
                                                'p-value':[round(tense_p_value,2),round(anxious_p_value,2),round(dispirited_p_value,2)],
                                                'significant improvemnt':['not sure','not sure','not sure']},
                                               index=['tense_relaxed_score','anxious_calm_score','dispirited_empowered_score'])
        

        if tense_p_value <= tense_threshold:
            significant_test_result.at['tense_relaxed_score','significant improvemnt'] = "Yes, there is a significant improvement in {confidence_level}% confidence level".format(confidence_level=int(100*(1-tense_threshold)))
        else:
            significant_test_result.at['tense_relaxed_score','significant improvemnt'] = "No, there is no significant improvement in {confidence_level}% confidence level".format(confidence_level=int(100*(1-tense_threshold)))
        if anxious_p_value <= anxious_threshold:
            significant_test_result.at['anxious_calm_score','significant improvemnt'] = "Yes, there is a significant improvement in {confidence_level}% confidence level".format(confidence_level=int(100*(1-anxious_threshold)))
        else:
            significant_test_result.at['anxious_calm_score','significant improvemnt'] = "No, there is no significant improvement in {confidence_level}% confidence level".format(confidence_level=int(100*(1-anxious_threshold)))
        if dispirited_p_value <= dispirited_threshold:
            significant_test_result.at['dispirited_empowered_score','significant improvemnt'] = "Yes, there is a significant improvement in {confidence_level}% confidence level".format(confidence_level=int(100*(1-dispirited_threshold)))
        else:
            significant_test_result.at['dispirited_empowered_score','significant improvemnt'] = "No, there is no significant improvement in {confidence_level}% confidence level".format(confidence_level=int(100*(1-dispirited_threshold)))
        
        significant_test_result.index = ['Tense - Relaxed','Anxious - Calm','Dispirited - Empowered']
        return significant_test_result
        
    def group_by(self, column, methods = ['counts'], metrics='tense', sort_by = 'tense', ascending = False):
        '''
        It will show the distribution of the data, which is group by different columns.
         
        def group_by( column, methods = ['counts','avg'], metrics=['all'], sort_by = 'tense', ascending = False)
        
        Parameters:
        
            column: It’s the column we want to check the distribution for, it could be one of the columns from the original table or one of the following: ‘day’, ‘month’ or ‘year’ which will group the data by the time. 
            
            methods: The aggregated function we want to use, can be ‘counts’ or ‘avg’. The default setting is to output both of them.
            
            metrics: It’s the metrics we want to check for. The default setting is ‘all’, which will show us all three metrics: tense_relaxed_change, anxious_calm_change and  dispirited_empowered_change. We can just input one or two of these metric, ‘tense’ for tense_relaxed_change, ‘anxious’ for anxious_calm_change and ‘dispirited’ for dispirited_empowered_change. If we want to show two metrics at once, we need to use the square brackets, like [‘tense’, ‘anxious’].
            
            ascending: The order of the final table. Default setting is False, which means the default order is descending. If we want to reverse the order, we can set it as True.

        Here is an example:
            # Check the distribution of tense_relaxed_change group by location
            report_generator.group_by('location','tense')
        Special case:
            # Show the change of users’ feeling
            report_generator.group_by('all')
        '''
        
        if 'counts' in methods:
            # initialization
            tense_relaxed_visual, anxious_calm_visual, dispirited_empowered_visual = [],[],[]

            if column == 'all':
                tense_relaxed_visual = pd.DataFrame(self.data.tense_relaxed_change.value_counts())
                anxious_calm_visual = pd.DataFrame(self.data.anxious_calm_change.value_counts())
                dispirited_empowered_visual = pd.DataFrame(self.data.dispirited_empowered_change.value_counts())
                display = pd.DataFrame(tense_relaxed_visual.join(anxious_calm_visual,how='outer').join(dispirited_empowered_visual,how='outer').fillna(0).astype('int'))
                display.index.name = 'score_changed'
                return display

            else:
                if 'tense' == metrics:
                    tense_relaxed_visual = pd.DataFrame(self.data.groupby(column).tense_relaxed_change.value_counts())
                    tense_relaxed_visual.columns = ['counts']
                    return pd.DataFrame(tense_relaxed_visual.sort_values([column,'counts'],ascending = ascending))
                elif 'anxious' == metrics:
                    anxious_calm_visual = pd.DataFrame(self.data.groupby(column).anxious_calm_change.value_counts())
                    anxious_calm_visual.columns = ['counts']
                    return anxious_calm_visual.sort_values([column,'counts'],ascending = ascending)
                elif 'dispirited' == metrics:
                    dispirited_empowered_visual = pd.DataFrame(self.data.groupby(column).dispirited_empowered_change.value_counts())
                    dispirited_empowered_visual.columns = ['counts']
                    return dispirited_empowered_visual.sort_values([column,'counts'],ascending = ascending)
#             if methods == 'counts':
#                 return tense_relaxed_visual, anxious_calm_visual, dispirited_empowered_visual
        
        if 'avg' in methods:
            if column == 'all':
                display = pd.DataFrame([self.data.tense_relaxed_pre.describe()['mean'],
                                        self.data.tense_relaxed_post.describe()['mean'],
                                        self.data.tense_relaxed_change.describe()['mean'],
                                        self.data.anxious_calm_pre.describe()['mean'],
                                        self.data.anxious_calm_post.describe()['mean'],
                                        self.data.anxious_calm_change.describe()['mean'],
                                        self.data.dispirited_empowered_pre.describe()['mean'],
                                        self.data.dispirited_empowered_post.describe()['mean'],
                                        self.data.dispirited_empowered_change.describe()['mean']],
                                       index=['tense_score_pre','tense_score_post','tense_score_change',
                                              'anxious_calm_pre','anxious_calm_post','anxious_calm_change',
                                            'dispirited_empowered_pre','dispirited_empowered_post','dispirited_empowered_change'],columns=['Average'])
                return display
            else:
                avg = pd.DataFrame(self.data.groupby(column).tense_relaxed_change.apply(np.average))
                avg.columns=['average_tense_change']
                avg['average_anxious_change'] = (self.data.groupby(column).anxious_calm_change.apply(np.average))
                avg['average_dispirited_change'] = pd.DataFrame(self.data.groupby(column).dispirited_empowered_change.apply(np.average))
                if sort_by == 'tense':
                    return avg.sort_values('average_tense_change',ascending=ascending)
                elif sort_by == 'anxious':
                    return avg.sort_values('average_anxious_change',ascending=ascending)
                elif sort_by == 'dispirited':
                    return avg.sort_values('average_dispirited_change',ascending=ascending)
                else:
                    return avg

    def group_significant_test(self, column, tense_threshold=0.05, anxious_threshold=0.05, dispirited_threshold=0.05):
        '''
         It can do the t-test for each column, check whether there is a statistical improvement. For example, if we want to check whether there is statistical improvement in different area, we can use this method.

        group_significant_test(column=None, tense_threshold=0.05, anxious_threshold=0.05, dispirited_threshold=0.05)

        Parameters:
        
            column: It’s the column we want to do the t-test for, it could be one of the columns from the original table or one of the following: ‘day’, ‘month’ or ‘year’ which will group the data by the time. However, we recommend to input a column which doesn’t have much different values.
           
            tense_threshold: The level of probability (alpha level, level of significance, p) we are going to use in the t-test for tense_relaxed experiment. The default setting is 0.05, which indicates a 5% risk of concluding that a difference exists when there is 	no actual difference.
           
            anxious_threshold: The level of probability (alpha level, level of significance, p) we are going to use in the t-test for anxious_calm experiment. The default setting is 0.05, which indicates a 5% risk of concluding that a difference exists when there is no actual difference.
             
            dispirited_threshold: The level of probability (alpha level, level of significance, p) we are going to use in the t-test for dispirited_empowered experiment. The default setting is 0.05, which indicates a 5% risk of concluding that a difference exists when there is no actual difference.

            Here is an example:
                # Check whether there is significant improvement in different areas
                report_generator.group_significant_test('location')
        '''
        
        # Initialization
        i = 0
        metrics_num = 3
        result_table = np.empty(shape=(len(self.data[column].value_counts().keys()),metrics_num),dtype=str)
        
        # Do the t-test for each value in the column
        for value in self.data[column].value_counts().keys():
            temp_data = self.data[self.data[column] == value]
            _, tense_p_value = stats.ttest_rel(self.data['tense_relaxed_pre'], self.data['tense_relaxed_post'],alternative='less')
            _, anxious_p_value = stats.ttest_rel(self.data['anxious_calm_pre'], self.data['anxious_calm_post'],alternative='less')
            _, dispirited_p_value = stats.ttest_rel(self.data['dispirited_empowered_pre'], self.data['dispirited_empowered_post'],alternative='less')
            if tense_p_value <= tense_threshold:
                result_table[i][0] = 1
            else:
                result_table[i][0] = 0
            if anxious_p_value <= anxious_threshold:
                result_table[i][1] = 1
            else:
                result_table[i][1] = 0
            if dispirited_p_value <= dispirited_threshold:
                result_table[i][2] = 1
            else:
                result_table[i][2] = 0
            i += 1
        
        # Convert the table into dataframe
        result = pd.DataFrame(result_table, index = self.data[column].value_counts().keys())
        result.columns = ['tense_relaxed_change', 'anxious_calm_change','dispirited_empowered_change']
        
        #print(result)
        return result


if __name__ == '__main__':
    # Title
    st.title('PNI Therapeutics')
    
    # Check the data we want, from 
    col1, col2, col3 = st.columns(3)
    col1.header("Time period")
    col2.text_input("From", key="date_from")
    col3.text_input("To", key="date_to")
    st.text_input("Headset number you want to check, put the number in parentheses like (1,2,3)", key="headset_num")
    if st.session_state.date_from and st.session_state.date_to:
        data_test = load_data(start_date=st.session_state.date_from,end_date=st.session_state.date_to,headset_num=st.session_state.headset_num)
    elif st.session_state.date_from:
        data_test = load_data(start_date=st.session_state.date_from,headset_num=st.session_state.headset_num)
    elif st.session_state.date_to:
        data_test = load_data(end_date=st.session_state.date_to,headset_num=st.session_state.headset_num)
    else:
        data_test = load_data(headset_num=st.session_state.headset_num)

    report = report_generator(data_test) 
    # The first part, show the average scores before using our application
    # Then show the average scores after using our application and show the improment
    # Before using our application

    col1, col2, col3 = st.columns(3)
    col1.metric(label="Before: Tense - Relexed",
              value=round(report.group_by('all',methods=['avg']).values[0][0],2))
    col2.metric(label="Before: Anxious - Calm",
              value=round(report.group_by('all',methods=['avg']).values[3][0],2))
    col3.metric(label="Before: Dispirited - Empowered",
              value=round(report.group_by('all',methods=['avg']).values[6][0],2))
    # After using our application
    col4, col5, col6 = st.columns(3)
    col4.metric(label="After: Tense - Relexed",
              value=round(report.group_by('all',methods=['avg']).values[1][0],2),
              delta=round(report.group_by('all',methods=['avg']).values[2][0],2))
    col5.metric(label="After: Anxious - Calm",
              value=round(report.group_by('all',methods=['avg']).values[4][0],2),
              delta=round(report.group_by('all',methods=['avg']).values[5][0],2))
    col6.metric(label="After: Dispirited - Empowered",
              value=round(report.group_by('all',methods=['avg']).values[7][0],2),
              delta=round(report.group_by('all',methods=['avg']).values[8][0],2))

    
    # The second part, show the result of the significant test
    # Significant test part
    st.write("Significant test:")
    st.dataframe(report.significant_test())
    
    
    # The third part, general infomation part: show some general statistical metrics
    # Show the basic analysis about number_of_times_used, VR_version
    col1, col2 = st.columns(2)
    with col1:
        st.write("Avg Improvement with VR version:")
        selection = alt.selection_multi(fields=['series'], bind='legend')
        df0 = report.group_by('VR_version',methods=['avg'])
        df0.columns = 'Tense - Relexed', 'Anxious - Calm','Dispirited - Empowered'
        df1 = pd.DataFrame({'VR_version':df0.index.values,'metrics':[df0.columns.values for _ in df0.index.values],'Avg Improvement':[x for x in df0.values]})
        df = df1.explode(['metrics','Avg Improvement'])

        c = alt.Chart(df).mark_bar(opacity=0.7).encode(
                                            x=alt.X('VR_version:N',axis=alt.Axis(labelAngle=0),title=None),
                                            y='Avg Improvement:Q',
                                            color=alt.Color('metrics:N',legend=None),
                                            column=alt.Column('metrics:N',title=None),).properties(width=80,height=150)
        c
    with col2:
        st.write("Avg Improvement with Number of times used:")
        df0 = report.group_by('number_of_times_used',methods=['avg'])
        df0.columns = 'Tense - Relexed', 'Anxious - Calm','Dispirited - Empowered'
        df1 = pd.DataFrame({'number_of_times_used':df0.index.values,'metrics':[df0.columns.values for _ in df0.index.values],'Avg Improvement':[x for x in df0.values]})
        df = df1.explode(['metrics','Avg Improvement'])
        
        c = alt.Chart(df).mark_bar(opacity=0.7).encode(
                                            x=alt.X('number_of_times_used:N',axis=alt.Axis(labelAngle=0),title=None),
                                            y='Avg Improvement:Q',
                                            color=alt.Color('metrics:N',legend=None),
                                            column=alt.Column('metrics:N',title=None),).properties(width=80,height=150)
        c
    
    # Show the basic analysis about the distribution of people's feeling
    st.write("Total Score Improvement Counts:")
    df0 = report.group_by(column='all',methods=['counts'],metrics='tense')
    df0.columns = 'Tense - Relexed', 'Anxious - Calm','Dispirited - Empowered'
    df1 = pd.DataFrame({'Score improvement':df0.index.values,'metrics':[df0.columns.values for _ in df0.index.values],'Counts':[x for x in df0.values]})
    df = df1.explode(['metrics','Counts'])
    
    c = alt.Chart(df).mark_bar(opacity=0.7).encode(
                                            x=alt.X('Score improvement:N',axis=alt.Axis(labelAngle=0),title=None),
                                            y='Counts:Q',
                                            color=alt.Color('metrics:N',legend=None),
                                            column=alt.Column('metrics:N',title=None),).properties(width=200,height=150)
    c
    
    
    
    
    # The fourth part, customized part, show the analysis about the columns we are interested in 
    # Include the t-test result group by the columns, the total counts and distribution
    # Get the column we want to check, if nothing input, show the analysis of VR version and number_of_times_used
    option = st.selectbox(
     'The column you want to check:',
     ('None','number_of_times_used','headset_num', 'VR_version', 'month','day','year','location'))
    
    # When we get input
    if option != 'None':
        # Basic statistics metric
        st.write("Basic analysis:")
        st.table(report.group_by(option,methods=['avg']).join(report.group_by(option,methods=['counts']).groupby(option).apply(sum)))
        # T test part
        group_t_test_df = report.group_significant_test(option)
        st.write("T test result for {column_name}:".format(column_name=option))
        st.table(group_t_test_df)
        # Group counts part
        st.write("Basic analysis(group by {column_name}):Count".format(column_name=option))
        df = report.group_by(option,methods=['counts'],metrics='tense').reset_index()
        c = alt.Chart(df).mark_bar().encode(x=alt.X(option+':N', axis=alt.Axis(labelAngle=0)),
                                            y=alt.Y('counts'),
                                            color='tense_relaxed_change'+':N',
                                           )
        st.altair_chart(c, use_container_width=True)
        # Group counts ratio part
        st.write("Basic analysis(group by {column_name}):Ratio".format(column_name=option))
        df = report.group_by(option,methods=['counts'],metrics='tense').reset_index()
        c = alt.Chart(df).mark_bar().encode(x=alt.X(option+':N', axis=alt.Axis(labelAngle=0)),
                                            y=alt.Y('counts',stack="normalize",title='percentage'),
                                            color='tense_relaxed_change'+':N',
                                           )
        st.altair_chart(c, use_container_width=True)
    # When we get None
    else:
        pass