# Delphi Stock Screener
A CLI stock screener based on the yahoofinance API.



This project aims to deliver flexible and extensible stock-screening utility in a command line environment.

The name is inspired by the [Delphic Oracle](https://www.britannica.com/topic/Delphic-oracle). As picking stocks to invest into is, in many ways, prophesizing.

## Technologies Used
* SQLite database
* JSON files for configuration and persistent non-stock data
* Python scripts responsible for all the logic and processing

## Usage:
* Make sure you have [Python](https://wiki.python.org/moin/BeginnersGuide/Download) installed
* Required packages, [install](https://docs.python.org/3/installing/index.html) these:
  * [pandas](https://pypi.org/project/pandas/)
  * [yfinance](https://pypi.org/project/yfinance/)
* [Clone the repository using git](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository), or download it manually 
* Navigate to the project folder in your terminal of choice
* Run `$ python main.py --help`

Here is an example way to complete the above steps:
`$ python --version`  
`$ pip install pandas`  
`$ pip install yfinance`  
`$ git clone https://github.com/nkct/delphi-stock-screener.git`  
`$ cd ./delphi-stock-screener`  
`$ python main.py --help`  

Running `$ python main.py d` will display basic stock information for Aple, Micosoft and Google 

## Contributions:
All contributions, issues, and messages are welcome! If you aren't sure about something or have any questions don't be afraid to contact me.
