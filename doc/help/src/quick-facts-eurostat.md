# Quick Facts


|  Keyword                      | Value                            |
| ----------------------------- | -------------------------------- |
| Format Type Identifier        | EEA.EUROSTAT                     |
| Requirements                  | FME Build 22618+                 |
| Reader/Writer                 | Reader                           |
| Licensing Level               | Professional and above           |
| Dependencies                  | Python 3.8+                      |
| Dataset Type                  | None                             |
| Feature Type                  | Dataflow name                    |
| Typical File Extensions       | Not applicable                   |
| Automated Translation Support | ???                              |
| User-Defined Attributes       | ???                              |
| Coordinate System Support     | Yes                              |
| Generic Color Support         | No                               |
| Spatial Index                 | No                               |
| Schema Required               | ???                              |
| Transaction Support           | ???                              |
| Geometry Type                 | ???                              |
| Encoding Support              | UTF-8                            |


Geometry Support???:

| Geometry       | Supported? |
| -------------- | ---------- |
| aggregate      | yes[^1]    |
| circles        | no         |
| circular arc   | no         |
| donut polygon  | yes        |
| elliptical arc | no         |
| ellipses       | no         |
| line           | yes        |
| none           | yes        |
| point          | yes        |
| polygon        | yes        |
| raster         | no         |
| solid          | no         |
| surface        | no         |
| text           | no         |
| z values       | yes        |

[^1]: Only homogeneous aggregates (MultiPoint, MultiLine...) but not heterogeneous ones.