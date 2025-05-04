"""
Data Input and Preprocessing
    read_all_sheets: Reads all sheets of an Excel file and combines them into a single DataFrame. Handles errors during file reading.
    preprocess_daily_effort_tracker: Extracts employee ID, calculates day of week, sorts, and removes duplicates from effort data. Handles errors during preprocessing.
    read_employee_mapping: Reads employee mapping from an Excel file. Handles errors during file reading.
    validate_dataframe: Checks if a DataFrame is empty or missing required columns. Prints warnings/errors.
Data Aggregation and Metrics
    build_timesheet_completion_dump: Identifies employees who completed/didn't complete timesheets on each date. Filters and processes data based on specified criteria. [MODIFY THE code to set up the right ime in Filter_date = "YYYY-MM-DD"]
    create_portfolio_date_pivot: Calculates and pivots timesheet completion percentage by portfolio and date. Handles data validation and formatting.
    preprocess_data: Merges effort and employee data, cleans, and prepares for analysis. Drops unnecessary columns.
    save_to_excel: Saves a DataFrame to an Excel file in memory (BytesIO object).
    validate_merged_dataframe: Checks for required columns in the merged DataFrame.
    calculate_metrics: Adds columns for timesheet completion and leave status.
    group_by_portfolio_date: Groups data by portfolio and date for aggregation.
    aggregate_metrics: Aggregates effort metrics (sums) by portfolio and date.
    calculate_percentages: Calculates effort percentages by category.
    calculate_effort: Calculates total effort for specific effort types.
    create_gap_column: Calculates the difference between total daily hours and effort.
    create_deviations: Identifies high/low deviations from expected effort.
    create_file_name: Generates a file name with a timestamp.
    create_excel_with_multiple_tabs: Creates an Excel file with multiple sheets from provided DataFrames.
    generate_summary_effort: Orchestrates the calculation of various effort metrics.
    aggregate_timesheet_data: Aggregates timesheet completion and effort percentages by portfolio.
Streamlit Application
    Overall: Creates a Streamlit web application to upload, process, and visualize effort data. Handles file uploads and data display.

How to run the application
    streamlit run EmployeeEffortAnalysis.py
    upload the timesheet file
    upload the employee mapping file
    
"""

import streamlit as st
import pandas as pd
from io import BytesIO
import datetime

def read_all_sheets(file) -> pd.DataFrame:
    """Read all sheets from an Excel file and concatenate into one DataFrame."""
    try:
        sheets = pd.read_excel(file, sheet_name=None)
        df = pd.concat(sheets.values(), ignore_index=True)
        return df
    except Exception as e:
        st.error(f"Error reading effort file: {e}")
        return pd.DataFrame()

def preprocess_daily_effort_tracker(all_effort: pd.DataFrame) -> pd.DataFrame:
    """create empid from the email id field"""
    if all_effort.empty:
        return pd.DataFrame()
    try:
        all_effort['empid']=all_effort['Email'].str.extract(r'(^[^@]+)')
        all_effort['empid'] = all_effort['empid'].astype(int)
        all_effort['day_of_week'] = all_effort['Date'].dt.dayofweek

        all_effort1 = all_effort.sort_values(by=["empid", "Date", "Completion time"], ascending=[True, True, False])
        all_effort2 = all_effort1.drop_duplicates(subset=["empid", "Date"], keep="first")

        return all_effort2
    except Exception as e:
        st.error(f"Error during preprocessing of daily effort tracker data: {e}")
        return pd.DataFrame()

def read_employee_mapping(file) -> pd.DataFrame:
    """Read employee mapping Excel file."""
    try:
        df = pd.read_excel(file)
        #print("emp mapping sheet", df.columns)
        return df
    except Exception as e:
        st.error(f"Error reading employee mapping file: {e}")
        return pd.DataFrame()

def validate_dataframe(df: pd.DataFrame, required_columns: list, df_name: str) -> bool:
    if df is None or df.empty:
        print(f"Warning: {df_name} is empty or None.")
        return False
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        print(f"Error: {df_name} is missing columns: {missing_cols}")
        return False
    return True


def build_timesheet_completion_dump(all_effort_processed: pd.DataFrame, emp_mapping: pd.DataFrame) -> pd.DataFrame:
    required_effort_cols = ['empid', 'Date']
    required_emp_cols = ['empid', 'Employee Name', 'Portfolio', 'location']

    if not (validate_dataframe(all_effort_processed, required_effort_cols, 'all_effort_processed') and
            validate_dataframe(emp_mapping, required_emp_cols, 'emp_mapping')):
        return pd.DataFrame()

    try:
        # Ensure correct data types
        all_effort_processed['Date'] = pd.to_datetime(all_effort_processed['Date'], errors='coerce')
        emp_mapping['empid'] = emp_mapping['empid'].astype(all_effort_processed['empid'].dtype)

        # Filter dates >= YYYY-MM-DD
        Filter_date = "YYYY-MM-DD"
        cutoff_date = pd.Timestamp(Filter_date)
        all_effort_processed = all_effort_processed[all_effort_processed['Date'] >= cutoff_date]

        # Drop invalid rows
        all_effort_processed = all_effort_processed.dropna(subset=['Date', 'empid'])

        # Group by date to get unique empids who completed timesheet
        completed = all_effort_processed.groupby('Date')['empid'].unique().reset_index()
        completed.rename(columns={'empid': 'completed_empids'}, inplace=True)

        records = []
        for _, row in completed.iterrows():
            date = row['Date']
            completed_empids = set(row['completed_empids'])

            # Employees who completed
            completed_emps = emp_mapping[emp_mapping['empid'].isin(completed_empids)][
                ['empid', 'Employee Name', 'Portfolio', 'location']].copy()
            completed_emps['Status'] = 'Completed'
            completed_emps['Date'] = date

            # Employees who did not complete
            not_completed_emps = emp_mapping[~emp_mapping['empid'].isin(completed_empids)][
                ['empid', 'Employee Name', 'Portfolio', 'location']].copy()
            not_completed_emps['Status'] = 'Not Completed'
            not_completed_emps['Date'] = date

            records.append(completed_emps)
            records.append(not_completed_emps)

        if not records:
            print("No effort records found after processing.")
            return pd.DataFrame()

        result_df = pd.concat(records, ignore_index=True)
        result_df = result_df.sort_values(['Date', 'Status', 'empid'])
        return result_df

    except Exception as e:
        print(f"Error in build_timesheet_completion_dump: {e}")
        return pd.DataFrame()


def create_portfolio_date_pivot(all_effort_processed: pd.DataFrame, emp_mapping: pd.DataFrame) -> pd.DataFrame:
    required_effort_cols = ['empid', 'Date']
    required_emp_cols = ['empid', 'Portfolio']

    if not (validate_dataframe(all_effort_processed, required_effort_cols, 'all_effort_processed') and
            validate_dataframe(emp_mapping, required_emp_cols, 'emp_mapping')):
        return pd.DataFrame()

    try:
        all_effort_processed['Date'] = pd.to_datetime(all_effort_processed['Date'], errors='coerce')
        all_effort_processed = all_effort_processed.dropna(subset=['Date', 'empid'])
        all_effort_processed['day_of_week'] = all_effort_processed['Date'].dt.day_name()

        # Filter dates >= 19 April 2025
        cutoff_date = pd.Timestamp('2025-04-19')
        all_effort_processed = all_effort_processed[all_effort_processed['Date'] >= cutoff_date]

        weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday','Saturday','Sunday']
        filtered_effort = all_effort_processed[all_effort_processed['day_of_week'].isin(weekdays)]
        # Format Date as DD/MM string
        filtered_effort['Date'] = filtered_effort['Date'].dt.strftime('%m/%d')
        completed_pairs = filtered_effort[['empid', 'Date']].drop_duplicates()

        merged = pd.merge(completed_pairs, emp_mapping[['empid', 'Portfolio']], on='empid', how='left')
        merged = merged.dropna(subset=['Portfolio'])

        total_emps = emp_mapping.groupby('Portfolio')['empid'].nunique()

        completed_counts = merged.groupby(['Portfolio', 'Date'])['empid'].nunique()

        percent_completed = completed_counts.div(total_emps, level='Portfolio') * 100

        # Round to no decimals
        percent_completed = percent_completed.round(0)

        pivot_df = percent_completed.reset_index().pivot(index='Portfolio', columns='Date', values='empid')

        # Sort columns by date
        pivot_df = pivot_df.reindex(sorted(pivot_df.columns), axis=1)

        # Reset index to make Portfolio a column (first column)
        pivot_df = pivot_df.reset_index()

        return pivot_df

    except Exception as e:
        print(f"Error in create_portfolio_date_pivot: {e}")
        return pd.DataFrame()

def preprocess_data(effort_df: pd.DataFrame, emp_df: pd.DataFrame) -> pd.DataFrame:
    """Merge and preprocess data for dashboard."""
    if effort_df.empty or emp_df.empty:
        return pd.DataFrame()
    try:
        merged = pd.merge(effort_df, emp_df, on='empid', how='left')
        merged['Date'] = pd.to_datetime(merged['Date'], errors='coerce')
        merged = merged.dropna(subset=['Date', 'empid', 'Name','Portfolio', 'location'])
        columns_to_drop = ['ID', 'Start time','Completion time','Email','Last modified time','Employee Name']
        mergedshort = merged.drop(columns_to_drop, axis=1)
        #print("mergedshort effort sheet", mergedshort.columns)
        return mergedshort
    except Exception as e:
        st.error(f"Error during preprocessing: {e}")
        return pd.DataFrame()

def save_to_excel(df: pd.DataFrame) -> BytesIO:
    """Save DataFrame to Excel in memory and return BytesIO object."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return output


def validate_merged_dataframe(df):
    required_columns = ['empid', 'Name','Date','Portfolio', 'location', 'where you on Leave on this day ?',
                        'RunIncident', 'RunOther', 'ChangeMain', 'ChangeOther',
                        'Learning', 'Asset', 'Other', 'Total_Effort', 'gap']

    if not all(column in df.columns for column in required_columns):
        return False
    return True


def calculate_metrics(df):
    df = df.copy()
    df['Number of People filled timesheet'] = df.apply(lambda row: 1 if row['Total_Effort'] >= 0 else 0, axis=1)
    df['Number of people in Leave'] = df.apply(lambda row: 1 if row['where you on Leave on this day ?'] == 'Yes' else 0, axis=1)
    return df


def group_by_portfolio_date(df):
    df = df.copy()
    df['Date'] = pd.to_datetime(df['Date'])
    grouped_df = df.groupby(['Portfolio', 'Date'])
    return grouped_df


def aggregate_metrics(grouped_df):
    result_df = grouped_df.agg({
        'Number of People filled timesheet': 'sum',
        'Number of people in Leave': 'sum',
        'Total_Effort': 'sum',
        'RunIncident': 'sum',
        'RunOther': 'sum',
        'ChangeMain': 'sum',
        'ChangeOther': 'sum',
        'Learning': 'sum',
        'Asset': 'sum',
        'Other': 'sum',
        'gap':'sum'
    }).reset_index()
    return result_df


def calculate_percentages(df):
    df = df.copy()
    df['RunIncident%'] = df.apply(
        lambda row: round((row['RunIncident'] / row['Total_Effort']) * 100) if row['Total_Effort'] > 0 else 0, axis=1).astype(int)
    df['RunOther%'] = df.apply(
        lambda row: round((row['RunOther'] / row['Total_Effort']) * 100) if row['Total_Effort'] > 0 else 0, axis=1).astype(int)
    df['ChangeMain%'] = df.apply(
        lambda row: round((row['ChangeMain'] / row['Total_Effort']) * 100) if row['Total_Effort'] > 0 else 0, axis=1).astype(int)
    df['ChangeOther%'] = df.apply(
        lambda row: round((row['ChangeOther'] / row['Total_Effort']) * 100) if row['Total_Effort'] > 0 else 0, axis=1).astype(int)
    df['Learning%'] = df.apply(
        lambda row: round((row['Learning'] / row['Total_Effort']) * 100) if row['Total_Effort'] > 0 else 0, axis=1).astype(int)
    df['Asset%'] = df.apply(
        lambda row: round((row['Asset'] / row['Total_Effort']) * 100) if row['Total_Effort'] > 0 else 0, axis=1).astype(int)
    df['Other%'] = df.apply(
        lambda row: round((row['Other'] / row['Total_Effort']) * 100) if row['Total_Effort'] > 0 else 0, axis=1).astype(int)
    return df

def calculate_effort(df,effort_columns, derived_column):
    df[derived_column] = df[effort_columns].sum(axis=1)
    return df

def create_gap_column(df,Total_DailyHour):
    df['gap'] = Total_DailyHour - df['Total_Effort']
    return df


def create_deviations(df):
    # Calculate the threshold for 20% deviation
    threshold = 8 * 0.2
    # Create new columns to track deviations
    df['deviation_high'] = df.apply(lambda row: 1 if row['gap'] > threshold else 0, axis=1)
    df['deviation_low'] = df.apply(lambda row: 1 if row['gap'] < -threshold else 0, axis=1)
    return df

def create_file_name():
    current_time = datetime.datetime.now()
    file_name = f"effortDump{current_time.day:02d}_{current_time.month:02d}_{current_time.hour:02d}{current_time.minute:02d}IST.xlsx"
    return file_name
def create_excel_with_multiple_tabs(output_file, *args):
    if len(args) % 2 != 0:
        raise ValueError("An even number of arguments must be provided")

    dataframes = args[::2]
    sheet_names = args[1::2]

    with pd.ExcelWriter(output_file) as writer:
        for df, sheet_name in zip(dataframes, sheet_names):
            df.to_excel(writer, sheet_name=sheet_name, index=False)

def generate_summary_effort(df):
    effort_columns=['Effort in hours for current cyber incident recovery']
    derived_column="RunIncident"
    merged1=calculate_effort(merged,effort_columns,derived_column)

    effort_columns = ['Effort in hours for other run activities [ a. past incident trend analysis b. problem management c. certificate management d. remedy clean up,]','SOP Validation/ rewrite']
    derived_column = "RunOther"
    merged2 = calculate_effort(merged1, effort_columns, derived_column)

    effort_columns = ['Effort in hours for customer development activities [a. customer backlog planning, refinement, b. progress with backlog which is an existing JIRA story c. Vulnerability remediation d. Automation (...']
    derived_column = "ChangeMain"
    merged3 = calculate_effort(merged2, effort_columns, derived_column)

    effort_columns = ['Code Refactor in git repo. (TDD, Logging, exception handling)','Observability dashboard development']
    derived_column = "ChangeOther"
    merged4 = calculate_effort(merged3, effort_columns, derived_column)

    effort_columns = ['MongoDB session','Dynatrace session','Java session','TDD, Design principle Pattern session','Kafka session','Software engineering E1']
    derived_column = "Learning"
    merged5 = calculate_effort(merged4, effort_columns, derived_column)

    effort_columns = ['Reusable assets, Playbook, Automation','White Paper']
    derived_column = "Asset"
    merged6 = calculate_effort(merged5, effort_columns, derived_column)

    effort_columns = ['Other Learning']
    derived_column = "Other"
    merged7 = calculate_effort(merged6, effort_columns, derived_column)

    effort_columns = ['RunIncident','RunOther','ChangeMain','ChangeOther','Learning','Asset','Other']
    derived_column = "Total_Effort"
    merged8 = calculate_effort(merged7, effort_columns, derived_column)

    Total_DailyHour = 8
    merged9 = create_gap_column(merged8,Total_DailyHour)

    merged10 = create_deviations(merged9)
    return merged10

def aggregate_timesheet_data(result_df1: pd.DataFrame) -> pd.DataFrame:
    # Columns to sum and calculate percentages for
    effort_cols = ['RunIncident', 'RunOther', 'ChangeMain', 'ChangeOther', 'Learning', 'Asset', 'Other', 'gap']

    # Group by Portfolio and aggregate
    grouped = result_df1.groupby('Portfolio').agg(
        number_of_days_filled=pd.NamedAgg(column='Date', aggfunc='nunique'),
        number_people_filled=pd.NamedAgg(column='Number of People filled timesheet', aggfunc='sum'),
        number_people_leave=pd.NamedAgg(column='Number of people in Leave', aggfunc='sum'),
        RunIncident_sum=pd.NamedAgg(column='RunIncident', aggfunc='sum'),
        RunOther_sum=pd.NamedAgg(column='RunOther', aggfunc='sum'),
        ChangeMain_sum=pd.NamedAgg(column='ChangeMain', aggfunc='sum'),
        ChangeOther_sum=pd.NamedAgg(column='ChangeOther', aggfunc='sum'),
        Learning_sum=pd.NamedAgg(column='Learning', aggfunc='sum'),
        Asset_sum=pd.NamedAgg(column='Asset', aggfunc='sum'),
        Other_sum=pd.NamedAgg(column='Other', aggfunc='sum'),
        gap_sum=pd.NamedAgg(column='gap', aggfunc='sum'),
        Total_Effort_sum=pd.NamedAgg(column='Total_Effort', aggfunc='sum')
    )

    # Calculate percentages based on Total_Effort_sum
    for col in effort_cols + ['Total_Effort']:
        if col == 'Total_Effort':
            grouped['Total_Effort_percentage'] = 100.0
        else:
            sum_col = col + '_sum'
            grouped[col + '%'] = (grouped[sum_col] / grouped['Total_Effort_sum'] * 100).fillna(0)

    # Calculate leave percentage
    grouped['leave_percentage'] = (
        (grouped['number_people_leave'] / grouped['number_people_filled'])
        .replace([float('inf'), -float('inf')], 0)
        .fillna(0) * 100
    )

    # Calculate utilisation percentage
    denominator = (grouped['number_people_filled'] - grouped['number_people_leave']) * 8
    grouped['utilisation_percentage'] = (
        (grouped['Total_Effort_sum'] / denominator)
        .replace([float('inf'), -float('inf')], 0)
        .fillna(0) * 100
    )

    # Reset index to make Portfolio a column
    result = grouped.reset_index()

    # Round percentages to 2 decimals
    percentage_cols = [col for col in result.columns if col.endswith('_percentage')]
    result[percentage_cols] = result[percentage_cols].round(2)

    return result



# --- Streamlit UI ---
st.title('Team Effort Recording Dashboard')

effort_file = st.file_uploader('Upload Team Effort Excel', type=['xlsx'])
emp_mapping_file = st.file_uploader('Upload Employee Mapping Excel', type=['xlsx'])

if effort_file and emp_mapping_file:
    all_effort = read_all_sheets(effort_file)
    all_effort_processed = preprocess_daily_effort_tracker(all_effort)
    emp_mapping = read_employee_mapping(emp_mapping_file)
    timesheet_completion_dump = build_timesheet_completion_dump(all_effort_processed, emp_mapping)
    portfolio_date_pivot = create_portfolio_date_pivot(all_effort_processed, emp_mapping)
    merged = preprocess_data(all_effort_processed, emp_mapping)
    merged10 = generate_summary_effort(merged)

    if not validate_merged_dataframe(merged10):
        raise ValueError("Invalid dataframe")

    merged11 = calculate_metrics(merged10)
    grouped_df = group_by_portfolio_date(merged11)
    result_df = aggregate_metrics(grouped_df)
    result_df1 = calculate_percentages(result_df)
    result_df2 = aggregate_timesheet_data(result_df1)

    file_name = create_file_name()
    create_excel_with_multiple_tabs(file_name, merged11, 'completed_dump', timesheet_completion_dump, 'CompletionStatus', result_df1,'EffortAnalysis',result_df2,'EffortAnalysis_PVT',portfolio_date_pivot,'portfolio_date_PVT')

    if not timesheet_completion_dump.empty:
        st.subheader('Effort completion dump')
        st.dataframe(timesheet_completion_dump)
        excel_data = save_to_excel(timesheet_completion_dump)
        st.download_button(
            label="Download Effort Status Summary Report as Excel",
            data=excel_data,
            file_name='timesheet_completion_dump.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    else:
        st.warning("No data to display. Please check your files.")

    if not portfolio_date_pivot.empty:
        st.subheader('Effort portfolio date pivot')
        st.dataframe(portfolio_date_pivot)
        excel_data = save_to_excel(portfolio_date_pivot)
        st.download_button(
            label="Download Effort Status merged Report as Excel",
            data=excel_data,
            file_name='portfolio_date_pivot.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    else:
        st.warning("No data to display. Please check your files.")

    if not merged.empty:
        st.subheader('Effort merged data')
        st.dataframe(merged)
        excel_data = save_to_excel(merged)
        st.download_button(
            label="Download Effort Status merged Report as Excel",
            data=excel_data,
            file_name='EffortDump.xlsx',
            mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    else:
        st.warning("No data to display. Please check your files.")

else:
    st.info("Please upload both files to proceed.")
