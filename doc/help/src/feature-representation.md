# Eurostat Feature Representation 

| Attribute Name   | Notes     | Contents                                                                             |
| ---------------- | --------- | ------------------------------------------------------------------------------------ |
| DATAFLOW         | string: UTF-8 | Source of data (organization), datflow name, version (official name).            |
| LAST UPDATE      | string: UTF-8 | Last data update of the dataflow.                                                |
| TIME_PERIOD      | string: UTF-8/32 bit integer | Observation date/time for current record. Often referred to as Dimension.                                                                                                            |
| OBS_VALUE        | 32 bit real/integer | The registered value for the corresponding time period.                    |
| OBS_FLAG         | string: UTF-8 | Observation status *1.                                                           |

*1 see: [User Attributes](./user-attributes.md)

<!---
# Eurostat Feature Representation (Format Attributes)

| Attribute Name         | Notes     | Contents                                                                         |
| ---------------------- | --------- | -------------------------------------------------------------------------------- |
| Eurostat_*1         | Read-only | Codelist value used in the chosen dataflow. |
| Eurostat_*1_fullName  | Read-only | Full name of the codelist value used in the chosen dataflow.                     |
| xxx | Read-only | yyy.                                         |

[*1]: Each dataflow can have 1:N amount of codelists. Therefore, not all individual parameters are listed here.

--->