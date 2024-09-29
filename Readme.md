# Prerequisites:

* Install required packages <br>
`pip install -r requirement.txt` <br>
<br>
* Add your Subscription-Key <br>
  You need to add your Subscription-Key in `secret.txt` file, follow this format:<br>
  Subscription-Key=YourSubscriptionKeyHere<br>
  Note: Donot add any space or empty line



# How to Run Pipeline:
`python main.py`

# How to change input address:
You can chnage input address in `main.py`, examples are given in `main.py`

    city = "Augsburg"
    street = "Katharinengasse"
    house_number = "13"
    zip_code = "86150"

# How to extend for more states:
### You can extend this pipeline for other states by making milinal changes as follow:<br>

* You need to add state configuration in `config.json`, please check available configuration example.<br>
* You need to extend logic in `get_tile_names()` method to get 1x1km Lidar tile name according to source website.<br>
* (optional) If source file is not '.laz or .zip' file then you need to extend your logic in `download_any_file()` method inside `downloader.py` file.

