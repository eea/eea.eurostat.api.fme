# Eurostat Feature Representation 

| Attribute Name   | Notes     | Contents                                                                             |
| ---------------- | --------- | ------------------------------------------------------------------------------------ |
| DATAFLOW         | string: UTF-8 | Source of data (organisation), datflow name, version (official name).            |
| LAST UPDATE      | string: UTF-8 | Last data update of the dataflow.                                                |
| TIME_PERIOD      | string: UTF-8/32 bit integer | Observation date/time for current record. Often reffered to as     Dimension.                                                                                                            |
| OBS_VALUE        | 32 bit real/integer | The registered value for the corresponding time period.                    |
| OBS_FLAG         | string: UTF-8 | Observation status *1.                                                           |

Each unique dataflow can have extra attributes (e.g. freq, geo, unit, sex). These attributes refer to the series-keys in a dataflow. They are automatically exposed for each dataflow. The values of these attributes are represented as a code from unique codelists.

[*1]: OBS_FLAG is also part of the code lists. This codelist can always be present to give more information about the obersvation value and is therefore always present.

**Example:**

| Attribute name        | Attribute Value | Codelist value |
| --------------------- | --------------- |--------------- |
|GEO                    | SE              | Sweden         |
|GEO                    | DK              | Denmark        |
|Freq                   | A               | Annually       |
|Freq                   | M               | Monthly        |

<!---
# Eurostat Feature Representation (Format Attributes)

| Attribute Name         | Notes     | Contents                                                                         |
| ---------------------- | --------- | -------------------------------------------------------------------------------- |
| Eurostat_*1         | Read-only | Codelist value used in the chosen dataflow. |
| Eurostat_*1_fullName  | Read-only | Full name of the codelist value used in the chosen dataflow.                     |
| xxx | Read-only | yyy.                                         |

[*1]: Each dataflow can have 1:N amount of codelists. Therefore, not all individual parameters are listed here.

--->