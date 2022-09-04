# Flatten_jsons_Transformed_load_with_Airflow

1. I thought to make conditions easier, I should flatten json file firstly
a. Determined outer columns of the json file.
b. Determined the inner columns which are in nested list (metrics).
c. Created dataframe with both of outer and nested columns.
d. Get rows of json file with counting outer columns and expect ‘channelname’ it’s a mandatory
column to depend on it as if I would calculate all element in data[‘result’] have already elements
of nested list also.
e. Used function ‘json_normalize’ function to flatten json file and you can read more about it from
here .
2. Make transformation and conditions which are required
a. Apply condition and as output it’s the same column (&#39;value&#39;), I make loop to create list of values
for this column to map counts of conditions with counts of choices.
b. The parameter ‘TV_project_parm’ would raise error as it’s not declared and initialized before,
I checked the source file
c. Initialized the columns of the transformed columns and I got on extra column as I will need it in
exercise_3 like ‘campaignId, platform, campaignName’
d. Load and export result as csv to check the output based on the conditions

### Exercise 3_Airflow load to csv:
1. My Dag consist from 4 tasks
a. Use previous functions to flattened_json to get columns
b. transform_Json to transform the data.
c. And load to csv based on selected columns only
d. And finally removed staged layers and transformed files, I will need it after loading data
