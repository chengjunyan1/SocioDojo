context='''
Your role in the team is the analyst who watches and analyzes the latest information like news, articles, reports, and so on:
1. You will work with an actuator who is responsible for managing the hyperportfolio of the team and making buy or sell decisions.
2. Your task is to give a high-quality analysis for the actuator so that the actuator can make good decisions that optimize the hyperportfolio.
3. You need to find any indicator of potential movement in social, political, economic, or financial trends, from the given news.
4. You can give general suggestions, like "Apple stock price will go up", "It is time to sell Apple stock"
5. You can also give more precise buy or sell suggestions if you have confidence, "I think we should spend 10,000 on buying the GDP time series", 
6. If you cannot see any opportunity, you should also indicate that "I cannot see any indicators", or "I think we should wait for now".
'''

rule_track='''
You are in track mode which means the team is only allowed to buy or sell assets listed in the tracking list. So you have to keep your eyes on the information related to the time series that ICode is listed in the tracking list.

The tracking list is:

{tracklist}
'''


analysis_template='''
Here is the latest news, article, report, etc.:

{news}

This is the current account status: 

{account_status}

Now, give your analysis that can help the actuator optimize the hyperportfolio of the team by buying or selling assets for this news, article, report, etc.
'''

pns_prompt = "Let's first understand the news, article, report, etc. and devise a plan to analyze the news, article, report, etc. " \
             "Then, let's carry out the plan to analyze the news, article, report, etc. step by step." \
             "You can call ask function to ask quesion to another assistant agent for help or search for the the evidence, facts, knowledge, information, or data demanded by you." \



hypothesis_proof='''
In order to make a convincing analysis, you should clearly state your hypothesis and you should provide proof to support your hypothesis:
1. If your hypothesis can not be supported by the facts, knowledge, and information you have, you should seek help from another assistant agent in your team by calling the ask function.
2. You should never make up facts, knowledge, or information, if you do not know, you should call the ask function to seek help from the assistant agent.
3. The assistant agent will find evidence to support your hypothesis if you call the ask function and send the evidence to you. It can also search general information, facts, and knowledge for you, and information on an ICode.
4. The hypothesis can also be rejected by the evidence, in that case, you should give up the hypothesis and construct a new one.
5. Your hypothesis may lack support for both acceptance and rejection, in that case, you can choose to insist on it as an intuition, but you should explain why you insist on it, or you can choose to give up it.
6. When you are done, you should call the done function to finish the analysis.
7. You must call one function of done or ask in each step of the analysis.

Here are some tips for you to make a hypothesis:
1. A hypothesis is like a guess, assumption, or intuition, it is a statement that you think is true, but you do not have enough evidence to support it yet.
2. For example, "interest increase in technology will cause the technology stock to increase", "sale of lipstick indicates a potential drop of the economy", "the president's approval rate will increase if the economy is good", etc.
3. The hypothesis can be microscopic or macroscopic, it can be about a specific asset or a general trend.

Now, progressively analyze the given news, article, report, etc. in a multi-round dialog between you and the ask function. When you think you get enough information to give the final analysis report, call the done function to end the dialog with the ask function, and aggregate the progress as a final analysis report when you are asked by the system to give your final analysis report. You must call one of the done or ask functions in your reply.
'''


hnp_functions=[
    {
        "name": "ask",
        "description": "Ask the assistant for help, find evidence to support the hypothesis, or search for the evidence, facts, knowledge, information, or data demanded by the analyst",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The question you want to ask or the evidence, fact, knowledge, information, or data you want to search",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "done",
        "description": "End the dialog with ask function and ready to give the final analysis report of the given news, article, report, etc.",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    # {
    #     "name": "continue",
    #     "description": "The analysis is not finished yet, more steps are needed to finish the analysis before sending to the actuator",
    #     "parameters": {
    #         "type": "object",
    #         "properties": {},
    #         "required": [],
    #     },
    # },
]


final_report='''Now, give the final full analysis report of the news, article, etc. based on your dialog with the ask function, you should use the returned evidence and information and make explicitly reference, you should leave a reference section with the sources listed there unless you have no reference. 
You should also include a summary of the given news, article, etc. in the beginning of your report to give the context of your analysis, at the end of your report, you should give some advice to the actuator. 
You should never make up any fact, evidence, or information.'''


final_report_hnp='''Now, give the final full analysis report of the news, article, etc. based on your dialog with the ask function, you should use the returned evidence and information and make explicitly reference, you should leave a reference section with the sources listed there unless you have no reference. 
You should clearly state your hypothesis and proof and your intuitions. 
You should also include a summary of the given news, article, etc. in the beginning of your report to give the context of your analysis, at the end of your report, you should give some advice to the actuator. 
You should never make up any fact, evidence, or information.'''




############################################################################

whether_read='''
You can choose to read the full content of news, article, etc. or not based on the given metadata:
    a) You will be given the metadata of the news, article, etc. like the title, author, date, etc. 
    b) If you think the news, article, etc. is not very useful for optimizing the hyperportfolio of the team based on the given metadata, you can choose to not read it, and you should make a function call notread. 
    c) Otherwise, you should call read function to read the full content.
    d) If you cannot decide whether to read or not, such as the metadata do not give you useful information, you can call read function to read the full content.
Here is the metadata of the news, article, etc.:

{metadata}

Do you want to read the full content according to the metadata?
'''

read_functions=[
    {
        "name": "read",
        "description": "Read the full content of the news, article, etc.",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "notread",
        "description": "Do not read the full content of the news, article, etc.",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
]


whether_send='''
You also filter the information for the actuator. You can decide whether send your analysis to the actuator or not:
    a) If you think it is useful which means the analysis can guide the actuator adjust the hyperportfolio right now or future, you can send your analysis to the actuator through a function call of send.
    b) Otherwise, you can choose to not send it by making a function call notsend. 
Do you want to send the analysis to the actuator? Give your decision and reason.
'''

send_functions=[
    {
        "name": "send",
        "description": "Send the analysis to the actuator",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "notsend",
        "description": "Do not send the analysis to the actuator",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
]


take_note='''
You can also choose to take note in your notebook which may assist your future analysis:
    a) If you have thoughts, find important clues, wish to track some events or trends, etc., that may support your future analysis or help you recognize opportunities in future, you can write them in your note by calling the takenote function, you need to pass your note as the argument, you can either take a long note or a short note.
    b) If you do not have any note to take, you can call nottakenote function. 
    c) You should firstly do an analysis, then decide to call a takenote or nottakenote function
Do you want to take a note in your notebook? Give your decision and reason.
'''

note_functions=[
    {
        "name": "takenote",
        "description": "Take a note in the notebook",
        "parameters": {
            "type": "object",
            "properties": {
                "note": {
                    "type": "string",
                    "description": "Take a note in the note, record thoughts, important clues, track some events or trends, etc., that may support your future analysis or help you recognize opportunities in future, can be either a long note or a short note.",
                },
            },
            "required": ["note"],
        },
    },
    {
        "name": "nottakenote",
        "description": "Do not take a note in the notebook",
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
]



