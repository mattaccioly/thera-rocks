# context
Thera is a startup that automates the open innovation process using AI. Basically we connect big corporates with startups with a challenge. They send us their challenge and we find the correct list of startups that could solve this problem. 
To make this work we need to ingest a lot of data from different sources to build the startup profile and feed the model to process and find the correct startup. The data that we need are:
    - name of the startup
    - website
    - linkedin
    - number of employees
    - where is located (country)
    - who are the founders
    - a brief description
    - a detailed description
    - what is the product
    - what are their clients
    - what are their success cases
    - the technology that they use
    - what is their industry
    - what are their client target
Right now, we are ingesting this data from many places, but mainly from a csv file that need to be normalized, from a scraper from the website and from a scraper from linkedin.
For our technology, we are adopting mostly tools from GCP, such as Big Query, where our OLAP database is hosted. 
# prompt
I need you to build for me this data pipeline to feed my database. We will provide a CSV file to be normalized. The other data sources, as I told you before will come from scrapers.
But, there's one problem with the scrapers: I need to optmize the usage. So, in order to do this, I need only to scrap the first page, put into a LLM model, probably Gemini, to see if the content is real or from some scam. If the LLM authorizes, we must continue to scrap the page. 
With this data, I need to send to a LLM to summarize and find the info that I said before that I need from the startup.
I don't know exactly what kind of transformations that will be necessary to accomplish this. 