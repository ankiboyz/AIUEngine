# list_of_mongo_statements.py

'''
Here, this module primarily stores the mongo statements  e.g. aggregation pipelines as strings; which are dynamically resolved in the
executor where these commands are executed.
IN order to prepare these statements so that they are resolved as f-strings during runtime, following needs to be done:
1. first create an aggregation pipeline using pymongo.
2. Ensure inside the pipeline all " double quotes are used and no single quotes are used; single quotes will be used to enclose entire thing from outside.
3. All the new line spaces are put in with backslash '\' (back-slash to put there without single quotes);
so that the multi-line is signalled.
4.In notepad++ Replace all { with {{
5. In notepad++ Replace all } with }}
6.Put curly braces covering the dynamic variable values to be resolved via f-strings during dynamic execution in
the caller.
7. Do not place any braces for the functions whose value need be resolved when the actual call happens eg datetime.datetime.utcnow().
8. Put string quotes before and after string values those will be resolved after f-string conversion eg run_id, function_id
9. Do not put any comments in the pipeline
'''

AG_PIPELINE_TFA02_IFA19_1_3 = '[{{"$match": {{"runID": {{"$eq": "{run_id}" }}\
                                                                 ,"GLT_is_this_realized": {{"$ne": "DONE"}} \
                                                                }}\
                                                     }},\
                              {{"$limit": {limit_for_recs_processing_in_one_iteration}}}\
                             ,{{"$addFields" : {{"GLT_lastUpdatedDateTime": datetime.datetime.utcnow()\
                                             , "GLT_is_this_realized": "IN-PROCESS"	\
                                              }}\
                              }}\
                             ,{{"$merge" : {{ "into": "{function_id}"\
                                          , "on": "_id" \
                                          , "whenMatched":[ \
                                                         {{"$addFields":\
                                                                    {{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"\
                                                                    ,"GLT_is_this_realized": "$$new.GLT_is_this_realized"\
                                                                    }}\
                                                         }}\
                                                        ]\
                                          , "whenNotMatched": "discard" }} \
                              }},\
                              ]'