Eurostat Reader Parameters

### Dataset
The Eurostat *Dataset*

When selecting the Dataflow using the browse button a dialog window with a tree structure of folders categorizing the datasets appear. Navigating through the folders will lead to a selectable Dataflow. 
When selecting the Reportnet Dataset using the browse-button, the value will be encoded for readability, e.g. "Air pollutants by source sector (source: EEA) [ENV_AIR_EMIS]", where ENV_AIR_EMIS corresponds to the dataset id.

When supplying the value for dataset dynamically, such as when used in a FeatureReader, it is valid to only specify the dataset id.


## Parameters

### Expose format attributes full name
Ticking the box will lead to coded values in attributes being translated.
Leaving the box unchecked will leave coded values as they are.


### Filter on time

Optional. 
Providing a value for Start Period will ensure that data with a Time_Period greater than or equal to the given value will be read.
Providing a value for End Period will ensure that data with a Time_Period less than or equal to the given value will be read.
Values should correspond to the format for Time_Period for the chosen DataFlow. <refer to suitable documentation>

### Filter on first N and last N observations
Optional.  
Integers greater than zero.
Providing values will restrict the reader to only fetch the first N Observations and/or the last N Observations of the DataFlow with consideration taken to the optional Filter on time.
