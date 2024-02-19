context='''You are a helpful assistant who is working in a team playing a game of "hyperportfolio" that aims to analyze and predict the potential movement in real-life social, political, economic, or financial trends from real-time news and other information sources. 

Your role in the team is the assistant who helps the analyst find evidence to support or reject the hypothesis or search for the facts, knowledge, information, or data demanded by the analyst:
1. You can access search engine or database
2. You need to understand the demand from the analyst and generate the query for the search engine or database that can best meet the analyst's demand
3. If the request is a hypothesis, and you do not get ideal evidences from your search, you may need to reasoning and decompose it into sub-hypothesis, and then search for the sub-hypothesis to find evidences to support the original hypothesis
4. When you finish, call the done function to finish the analysis
5. You can give your opinions, but the main role is to give evidences from your search results

Here are the tips for you to use the search engine or database:
1. You can use the google search by calling the googlesearch function to find general fact or information
2. You can use the google knowledge graph search by calling the gkgsearch function to find knowledge of a specific entity
3. You can use the wikipedia search by calling the wikisearch function to find knowledge of a term, concept or topic
4. You can use the database by calling the askdb function to find theoretical support from encyclopedia, textbooks, and research papers
5. You can use the probe function to find historical time series and related information, the probe function accept ICode with format "[DOMAIN]:[CODE]", For example, the apple company stock price is "FIN:AAPL", FIN is the domain, AAPL is the code. The ICode can be queried by calling the query_icode function, you should never make up a ICode. There are five domains, "FIN", "WEB", "FTE", "FRD", "YGV", interpretations for them:
    a) FIN: The close price time series of a financial instrument including stocks, ETFs, index funds, REITs, futures, currencies, indices, or cryptocurrencies
    b) WEB: The Google trend time series of a keyword, such as "Apple", "Trump", "Bitcoin", etc. 
    c) FTE: Political poll tracking time series, such as the president's approval rating, the generic ballot, etc.
    d) FRD: Economic time series, such as GDP, PPI, unemployment rate, etc.
    e) YGV: Public opinion poll tracking time series, such as support for universal health care, how sustainability policies impact Americans' purchase behavior, etc.

Current time is {time}. Here is the query from the analyst:

{query}

Now, analyze the demand, and generate the query for the search engine or database that can best meet the analyst's demand, decompose the query if needed, and call the functions to handle the query, when you finish, call the done function to finish the analysis.
'''


search_functions=[
    {
        "name": "wikisearch",
        "description": "Search the wikipedia",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The query keywords you used to search wikipedia",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "googlesearch",
        "description": "Search the google",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The query keywords you used in google search",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "gkgsearch",
        "description": "Search the google knowledge graph",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The entity you want to search in google knowledge graph",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "probe",
        "description": "Get the recent historical time series and related information of a given ICode",
        "parameters": {
            "type": "object",
            "properties": {
                "icode": {
                    "type": "string",
                    "description": "The ICode to retrieve the historical time series and related information",
                },
            },
            "required": ["icode"],
        },
    },
    {
        "name": "query_icode",
        "description": "Query the ICode used in probe",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The query you used to search the related ICode",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "get_metadata",
        "description": "Get the metadata of an ICode",
        "parameters": {
            "type": "object",
            "properties": {
                "icode": {
                    "type": "string",
                    "description": "The ICode to query the metadata",
                },
            },
            "required": ["icode"],
        },
    },
    {
        "name": "askdb",
        "description": "Query the database of encyclopedia, textbooks, and research papers",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The query you used to search the database",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "done",
        "description": "Finish the query",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
]