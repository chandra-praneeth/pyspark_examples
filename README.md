# pyspark_examples
PySpark examples
#### Setup
* Create and initialise virtual environment(venv) 
  * Install venv package: `python3 -m pip install --user virtualenv`
  * Create a folder `env` using `mkdir env`
  * Run`python3 -m venv env` to create virtual environment
  * Activate virtual env `source env/bin/activate`

* Install dependencies from `requirements.txt` file
  * `python3 -m pip install -r requirements.txt`

### Run
  * In project root directory run `python3 src/main_csv.py`
  * To run unit tests, run command `pytest`

#### References
  * Virtual env: https://packaging.python.org/en/latest/guides/installing-using-pip-and-virtual-environments/
  * PySpark installation: https://spark.apache.org/docs/latest/api/python/getting_started/install.html
  * Getting started with PySpark: https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html
  * Java download: https://www.java.com/en/