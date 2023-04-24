# User Attributes

Each unique dataflow can have extra attributes (e.g. freq, geo, unit, sex). These attributes refer to the series-keys in a dataflow. They are automatically exposed for each dataflow. The values of these attributes are represented as a code from unique codelists.

[*1]: OBS_FLAG is also part of the code lists. This codelist can always be present to give more information about the obersvation value and is therefore always present.

**Example:**

| Attribute name        | Attribute Value | Codelist value |
| --------------------- | --------------- |--------------- |
|GEO                    | SE              | Sweden         |
|GEO                    | DK              | Denmark        |
|Freq                   | A               | Annually       |
|Freq                   | M               | Monthly        |

<!--- ### Getting the CodeList values --->
<!--- In order to get the full code list value from the attribute values, the following custom transformer can be used; xxx --->
