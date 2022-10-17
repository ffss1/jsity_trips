### Usage
`python jsity_trips.py [-init] [-f <csv path>] [-aw [-r <region>|-bbox <x1,y1,x2,y2>]]`
#### Arguments 
- **-init** initializes the SQLite database
- **-f trips.csv** loads trips.csv to database
- **-aw** prints the average weekly trips per region. Optionally:
	- **-r Turin** - filters trips by a region name
	- **-bbox 3.5,0,7,1.1** - filters by a bounding box
* Example: `python jsity_trips.py -init -f trips.csv -aw`

### Notes
* Requires `spatialite` module to be installed.
	* For Win, download binaries zip and extract at C:/Windows/system32 @ https://www.gaia-gis.it/gaia-sins/index.html, and:
	* `pip install spatialite`
