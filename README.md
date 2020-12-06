# A project for genhack hackathon

## How to use

First install required packages
`pip install -r requirements.txt`

In order to run the program, redis should run on a machine.  
Redis port and host can be configured in `src/redis_config.json`
 
You may copy our index from [google disk](https://drive.google.com/file/d/1hJpmM6evHRwDA4EqdNTrImif73JRAf_O/view?usp=sharing) or build your own.

A tool can be used with several commands:
Use `python main.py --find "your keywords"` to search through the index  
Or `python main.py --build` to add new articles to index
