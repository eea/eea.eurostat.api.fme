# Eurostat Feature Representation (Format Attributes)

| Attribute Name         | Notes     | Contents                                                                         |
| ---------------------- | --------- | -------------------------------------------------------------------------------- |
| Eurostat_*1         | Read-only | Codelist value used in the chosen dataflow. |
| Eurostat_*1_fullName  | Read-only | Full name of the codelist value used in the chosen dataflow.                     |
| xxx | Read-only | yyy.                                         |

[*1]: Each dataflow can have 1:N amount of codelists. Therefore, not all individual parameters are listed here.

**Example:**

| Attribute Name        | Attribute Value |
| --------------------- | --------------- |
|Eurostat_GEO           | SE              |
|Eurostat_GEO_fullName  | Sweden          |
|Eurostat_FREQ          | A               |
|Eurostat_FREQ_fullName | Annually        |