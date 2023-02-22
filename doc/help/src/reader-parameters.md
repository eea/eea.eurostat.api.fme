Eurostat Reader Parameters

### Dataset

To choose a dataset: 
- Click on the down arrow on the right side of the dataset.
- Choose "Select File From Web".
- Click on "Browse Eurostat (eea.eurostat)".

![SelectFileFromWeb.png](./Images/SelectFileFromWeb.png)



A dialog window with a tree structure of folders categorizing the datasets will appear. Navigating through the folders will lead to a selectable dataset. 
Only 1 dataset can be chosen at a time.

![DataBrowser.png](./Images/DataBrowser.png)

Select a dataset and click on "ok".

## Parameters

### Filter on time

**Optional.** 
Providing a value for Start Period will ensure that data with a Time_Period greater than or equal to the given value will be read.
Providing a value for End Period will ensure that data with a Time_Period less than or equal to the given value will be read.
Values should correspond to the format for Time_Period for the chosen DataFlow. <refer to suitable documentation>

|  Period       | Format                         |
| ------------- | ------------------------------ |
| Annual        | YYYY-A1 or YYYY                |
| Semester      | YYYY-S[1-2]                    |
| Quarter       | YYYY-Q[1-4]                    | 
| Monthly       | YYYY-M[01-12] or YYYY-[01-12]  | 
| Weekly        | YYYY-W[01-53]                  | 
| Daily         | YYYY-D[001-366]                | 
| Year interval | YYYY/P[01-99]Y                 | 

### Filter on first N and last N observations
Optional.  
This allows filtering data observations within a time series to limit the number of observations returned.
Providing values will restrict the reader to only fetch the first N Observations and/or the last N Observations of the DataFlow with consideration taken to the optional Filter on time.
-  This filter may be applied to data which has been filtered by dimension and / or time.
- First N Observations return the first N observations in the corresponding series
- Last N Observations return the last N observations in the corresponding series
- Both definitions may be applied e.g. to return the first and last observations in a series.
- If the filter is wrong, then a response is returned with no observation results.


### Expose format attributes full name
Ticking the box will lead to coded values in attributes being translated.
Leaving the box unchecked will leave coded values as they are.