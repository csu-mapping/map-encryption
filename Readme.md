Here is the relevant code and data of the paper "A local encryption method for large-scale vector maps based on spatial hierarchical index and 4D hyperchaotic system"

- The data folder contains the data used for the experiment, including points, lines, and polygons in shp format files.

- The src folder contains the key cryptographic code for the experiment.

  - vector folder contains PointEncryptBySPIndex, LineEncryptBySPIndex, PolygonEncryptBySPIndex. these files are respectively point, line, polygon encryption logic code part.
    - The kernel folder contains the kernel coordinate encryption conversion functions.

  - The lib folder contains the sm3 encryption code and other functions for converting data to different formats.

  - The db folder contains the code to connect to the database and store the results.