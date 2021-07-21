# list_of_mongo_statements.py

'''
Here, this module primarily stores the mongo statements  e.g. aggregation pipelines as strings; which are dynamically resolved in the
executor where these commands are executed.
IN order to prepare these statements so that they are resolved as f-strings during runtime, following needs to be done:
1. first create an aggregation pipeline using pymongo.
2. Ensure inside the pipeline all " double quotes are used and no single quotes are used; single quotes will be used to enclose entire thing from outside.
   It's recommended to Ctrl+F the single quote - to be firmly sure there is no single quote.
3. All the new line spaces are put in with backslash '\' (back-slash to put there without single quotes);
so that the multi-line is signalled.
4.In notepad++ Replace all { with {{
5. In notepad++ Replace all } with }}
6.Put curly braces covering the dynamic variable values to be resolved via f-strings during dynamic execution in
the caller.
7. Do not place any braces for the functions whose value need be resolved when the actual call happens eg datetime.datetime.utcnow().
8. Put string quotes before and after string values those will be resolved after f-string conversion eg as "{run_id}", "{function_id}"
9. Do not put any comments in the pipeline. It is recommended to Ctrl+F the occurrences of #.
10. Include the pipeline code as mentioned here between [].
11. The value of the pipeline variable here eg AG_PIPELINE_TFA02_IFA19_1_3 is enclosed within single quotes '' as shown below.
'''

AG_PIPELINE_GENERIC_STEP_3_MARK_ONES_TO_PROCESS = '[{{"$match": {{"runID": {{"$eq": "{run_id}" }}\
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

AG_PIPELINE_TFA02_IFA19_1_4 = '[{{"$match": {{"runID": {{"$eq": "{run_id}"}},\
                                            "GLT_is_this_realized": "IN-PROCESS"}}}}\
                             ,{{"$project": {{"_id": 0, "GLT_is_this_realized": 0}}}}\
                             ,{{"$addFields" : {{\
                                             "GLT_whether_S2A": {{"$cond": {{"if": {{"$and": [{{"$eq":["$EXCEPTION", "S2"]}},{{"$or":[{{"$eq":["$KOSTL_VALID", "TRUE"]}},{{"$eq":["$CAUFN_VALID", "TRUE"]}},{{"$eq":["$POSNR_VALID", "TRUE"]}}]}}]}}, "then": True,"else": False}}\
                                                              }}\
                                             , "status": "Unassigned"\
                                             , "control_id": "{control_id}"\
                                             , "GLT_history_runID" : {{"$concatArrays":[[{{"runID": "$runID", "GLT_lastUpdatedDateTime": "$GLT_lastUpdatedDateTime"}}],{{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}}\
                                             , "exceptionID" : "" \
                                             , "reason_code": "" \
                                             }}\
                              \
                              }}\
                             ,{{"$addFields" : {{\
                                            "GLT_do_auto_close": {{"$cond": {{"if": {{"$or": [{{"$eq":["$GLT_whether_S2A", True]}},{{"$eq":["$RATING", "LOW"]}}]}}, "then": True,"else": False}}}}\
                                             }}\
                              }}\
                             ,{{"$merge" : {{ "into": "{exception_collection_name}"\
                                          , "on": "COMPOSITEKEY"\
                                          , "whenMatched":[ \
                                                         {{"$addFields":\
                                                                    {{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"\
                                                                    ,"runID": "$$new.runID"\
                                                                    ,"GLT_do_auto_close":"$$new.GLT_do_auto_close"\
                                                                    ,"GLT_whether_S2A": "$$new.GLT_whether_S2A"\
                                                                    ,"GLT_history_runID": {{"$concatArrays":["$$new.GLT_history_runID", {{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}}\
                                                                    ,"exceptionID": {{"$toString": "$_id"}}\
                                                                    }}\
                                                         }}\
                                                        ]\
                                          , "whenNotMatched": "insert" }} \
                              }}\
                              ]'

AG_PIPELINE_TFA02_IFA19_SC7_1_3 = '[{{"$match": {{"runID": {{"$eq": "{run_id}" }}\
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

AG_PIPELINE_TFA02_IFA19_SC7_1_4 = '[{{"$match": {{"runID": {{"$eq": "{run_id}"}},\
									 "GLT_is_this_realized": "IN-PROCESS"}}}}\
						            ,{{"$project": {{"_id": 0, "GLT_is_this_realized": 0}}}}\
                                    ,{{"$addFields" : {{\
                                                       "status": "Unassigned"\
                                                     , "control_id": "{control_id}" \
                                                     , "GLT_history_runID" : {{"$concatArrays":[[{{"runID": "$runID", "GLT_lastUpdatedDateTime": "$GLT_lastUpdatedDateTime"}}],{{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}}\
                                                     , "exceptionID" : ""\
                                                     , "reason_code": ""\
                                                     }}\
                                      }}\
						            ,{{"$merge" : {{ "into": "{exception_collection_name}"\
									  , "on": "COMPOSITEKEY"\
									  , "whenMatched":  "keepExisting"\
									  , "whenNotMatched": "insert" }}\
						             }}\
						           ]'

AG_PIPELINE_TRE07_1_4 = '[{{"$match": {{"runID": {{"$eq": "{run_id}"}},\
                                        "GLT_is_this_realized": "IN-PROCESS"}}}}\
                         ,{{"$project": {{"_id": 0, "GLT_is_this_realized": 0}}}}\
                         ,{{"$addFields" : {{\
                                           "status": "Unassigned"\
                                         , "control_id": "{control_id}"\
                                         , "GLT_history_runID" : {{"$concatArrays":[[{{"runID": "$runID", "GLT_lastUpdatedDateTime": "$GLT_lastUpdatedDateTime"}}],{{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}}\
                                         , "exceptionID" :""\
                                         , "reason_code":""\
                                         , "GLT_do_auto_close": False\
                                         , "GLT_do_auto_reopen": False\
                                         }} \
                          }}\
                         ,{{"$merge" : {{ "into": "{exception_collection_name}"\
                                      , "on": "COMPOSITEKEY"\
                                      , "whenMatched":[\
                                                     {{"$addFields":\
                                                                {{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"\
                                                                ,"runID": "$$new.runID"\
                                                                ,"GLT_do_auto_close":{{"$cond":{{"if":{{"$eq":["$$new.EXP_STATUS", "Cleared Item"]}}, "then":True, "else":False}}}}\
                                                                ,"GLT_do_auto_reopen":{{"$cond":{{"if":{{"$and":[{{"$eq":["$$new.EXP_STATUS", "Open Item"]}},{{"$eq":["$status", "Closed"]}}]}}, "then":True, "else":False}}}}\
                                                                ,"GLT_history_runID": {{"$concatArrays":["$$new.GLT_history_runID", {{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}}\
                                                                ,"exceptionID": {{"$toString": "$_id"}}\
                                                                }}\
                                                     }}\
                                                    ]\
                                      , "whenNotMatched": "insert" }}\
                          }}\
                          ]'

AG_PIPELINE_FIN08_FA_1_4 = '[{{"$match": {{"runID": {{"$eq": "{run_id}"}},\
                                           "GLT_is_this_realized": "IN-PROCESS"}}}}\
                             ,{{"$project": {{"_id": 0, "GLT_is_this_realized": 0}}}}\
                             ,{{"$addFields" : {{\
                                                 "status": "Unassigned"\
                                               , "control_id": "{control_id}"\
                                               , "GLT_history_runID" : {{"$concatArrays":[[{{"runID": "$runID", "GLT_lastUpdatedDateTime": "$GLT_lastUpdatedDateTime"}}],{{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}}\
                                               , "exceptionID" : ""\
                                               , "reason_code": ""\
                                               , "GLT_do_auto_close": {{"$cond":{{"if":{{"$eq":["$IMDIF", "X"]}}, "then":True, "else":False}}}}\
                                               , "GLT_do_auto_reopen": False\
                              }} \
                             }}\
                             ,{{"$merge" : {{ "into": "{exception_collection_name}"\
                                             , "on": "COMPOSITEKEY"\
                                             , "whenMatched":[ \
                                                              {{"$addFields":\
                                                                {{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"\
                                                                 ,"GLT_do_auto_close":"$$new.GLT_do_auto_close"\
                                                                 ,"GLT_do_auto_reopen":False\
                                                                 ,"GLT_history_runID": {{"$concatArrays":["$$new.GLT_history_runID", {{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}}\
                                                                 ,"exceptionID": {{"$toString": "$_id"}}\
                                                               }}\
                                                               }}\
                                                              ]\
                                             , "whenNotMatched": "insert" }}\
                             }}\
                             ]'