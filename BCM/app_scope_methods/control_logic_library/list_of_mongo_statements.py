# list_of_mongo_statements.py

'''
Here, this module primarily stores the mongo statements  e.g. aggregation pipelines as strings; which are dynamically resolved in the
executor where these commands are executed.
IN order to prepare these statements so that they are resolved as f-strings during runtime, following needs to be done:
Follow it in the below sequence only:
1. first create an aggregation pipeline using pymongo.
2. Do not put any comments in the pipeline. It is recommended to Ctrl+F the occurrences of # and remove them.
3. Ensure inside the pipeline all " double quotes are used and no single quotes are used; single quotes will be used to enclose entire thing from outside.
   It's recommended to Ctrl+F the single quote - to be firmly sure there is no single quote.
4. All the new line spaces are put in with backslash '\' (back-slash to put there without single quotes);
so that the multi-line is signalled.
5.In notepad++ Replace all { with {{
6. In notepad++ Replace all } with }}
7.Put curly braces covering the dynamic variable values to be resolved via f-strings during dynamic execution in
the caller e.g. run_id , function_id as {run_id} and {function_id}.
8. It needs to be ensured that the dynamic variables i.e. those put under curly braces as of step 6 need to be resolved from the code.
9. Do not place any braces for the functions whose value need be resolved when the actual call happens eg datetime.datetime.utcnow().
10. Put string quotes before and after string values those will be resolved after f-string conversion eg as "{run_id}", "{function_id}"
here, the values that need to be resolved as NUMERIC eg limit_for_recs_processing_in_one_iteration should NOT be enclosed in double quotes ""
11. Include the pipeline code as mentioned here between [].
12. The value of the pipeline variable here eg AG_PIPELINE_TFA02_IFA19_1_3 is enclosed within single quotes '' as shown below '[....]'.
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
																	,"FILENAME":"$$new.FILENAME"\
                                                                    ,"GLT_do_auto_close":"$$new.GLT_do_auto_close"\
                                                                    ,"GLT_whether_S2A": "$$new.GLT_whether_S2A"\
                                                                    ,"GLT_history_runID": {{"$concatArrays":["$$new.GLT_history_runID", {{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}}\
                                                                    ,"exceptionID": {{"$toString": "$_id"}}\
																	,"USNAM":"$$new.USNAM"\
																	,"NAME_TEXT":"$$new.NAME_TEXT"\
																	,"KOSTL":"$$new.KOSTL"\
																	,"KOSTL_VALID":"$$new.KOSTL_VALID"\
																	,"CAUFN":"$$new.CAUFN"\
																	,"CAUFN_VALID":"$$new.CAUFN_VALID"\
																	,"POSNR":"$$new.POSNR"\
																	,"PNR_VALID":"$$new.PNR_VALID"\
																	,"UDATE":"$$new.UDATE"\
																	,"UTIME":"$$new.UTIME"\
																	,"TCODE":"$$new.TCODE"\
																	,"CNGID":"$$new.CNGID"\
																	,"VALUE_NEW":"$$new.VALUE_NEW"\
																	,"VALUE_OLD":"$$new.VALUE_OLD"\
																	,"WAERS":"$$new.WAERS"\
																	,"AVCC":"$$new.AVCC"\
																	,"AVEUR":"$$new.AVEUR"\
																	,"RATING":"$$new.RATING"\
																	,"EXCEPTION":"$$new.EXCEPTION"\
																	,"CDHDR_OBJCLAS":"$$new.CDHDR_OBJCLAS"\
																	,"CDHDR_OBJCTID":"$$new.CDHDR_OBJCTID"\
																	,"CDHDR_CHANGENR":"$$new.CDHDR_CHANGENR"\
																	,"CDPOS_OBJCLAS":"$$new.CDPOS_OBJCLAS"\
																	,"CDPOS_OBJTID":"$$new.CDPOS_OBJTID"\
																	,"CDPOS_CHANGENR":"$$new.CDPOS_CHANGENR"\
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
                                         , "GLT_is_this_fresh": True\
                                         }} \
                          }}\
                         ,{{"$merge" : {{ "into": "{exception_collection_name}"\
                                      , "on": "COMPOSITEKEY"\
                                      , "whenMatched":[\
                                                     {{"$addFields":\
                                                                {{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"\
                                                                ,"runID": "$$new.runID"\
                                                                ,"FILENAME":"$$new.FILENAME"\
                                                                ,"GLT_do_auto_close":{{"$cond":{{"if":{{"$eq":["$$new.EXP_STATUS", "Cleared Item"]}}, "then":True, "else":False}}}}\
                                                                ,"GLT_do_auto_reopen":{{"$cond":{{"if":{{"$and":[{{"$eq":["$$new.EXP_STATUS", "Open Item"]}},{{"$eq":["$status", "Closed"]}}]}}, "then":True, "else":False}}}}\
                                                                ,"GLT_history_runID": {{"$concatArrays":["$$new.GLT_history_runID", {{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}}\
                                                                ,"GLT_is_this_fresh": False\
																,"exceptionID": {{"$toString": "$_id"}}\
																,"XOPVW":"$$new.XOPVW"\
																,"AGE":"$$new.AGE"\
																,"EXP_STATUS":"$$new.EXP_STATUS"\
                                                                ,"AUGDT":"$$new.AUGDT"\
                                                                ,"AUGBL":"$$new.AUGBL"\
																,"XSTOV":"$$new.XSTOV"\
																,"XREVERSAL":"$$new.XREVERSAL"\
																,"XRAGL":"$$new.XRAGL"\
																,"ZUONR":"$$new.ZUONR"\
																,"XBLNR1":"$$new.XBLNR1"\
																,"SGTXT":"$$new.SGTXT"\
																,"RATING":"$$new.RATING"\
																,"BLART":"$$new.BLART"\
																,"XBLNR":"$$new.XBLNR"\
																,"USNAM":"$$new.USNAM"\
																,"CPUDT":"$$new.CPUDT"\
																,"CPUTM":"$$new.CPUTM"\
                                                                }}\
                                                     }}\
                                                    ]\
                                      , "whenNotMatched": "insert" }}\
                          }}\
                          ]'

AG_PIPELINE_FIN08_FA_1_3 = '[{{"$match": {{"runID": {{"$eq": "{run_id}" }}\
                                           ,"GLT_is_this_realized": {{"$ne": "DONE"}} \
                                           }}\
                                           }}\
						 ,{{"$limit": {limit_for_recs_processing_in_one_iteration}}}\
						 ,{{"$addFields" : {{"GLT_lastUpdatedDateTime": datetime.datetime.utcnow()\
										 , "GLT_is_this_realized": "IN-PROCESS"	 \
										  }}\
						  }}\
                         ,{{"$lookup": \
                           {{\
                               "from":"{exception_collection_name}"\
                              , "let": {{"foreignField":"$COMPOSITEKEY"}}\
                              ,"pipeline": [\
                                              {{"$match": \
                                               {{"$expr":\
                                                   {{ "$eq": [ "$COMPOSITEKEY",  "$$foreignField" ] }}\
                                               }}\
                                              }}\
                                             ,{{"$sort" : {{"GLT_incremental_number": -1}}}}                         \
                                           ]\
                              ,"as": "matched_existing_exception"\
                           }}\
                          }},\
                          {{"$addFields":{{"firstelem":{{"$first":"$matched_existing_exception"}}}}\
                            }},\
                          {{"$addFields":{{"GLT_exception_status":"$firstelem.status",\
                                         "GLT_incremental_number":{{"$ifNull":["$firstelem.GLT_incremental_number",1]}} \
                                        }}\
                          }},\
                          {{"$addFields":{{\
                                          "GLT_do_exception_exist":{{"$cond":\
                                                                    {{"if":{{\
                                                                        "$eq":[{{"$ifNull":["$GLT_exception_status","X"]}}, "X"]\
                                                                          }}, \
                                                                     "then":\
                                                                         False, \
                                                                     "else":\
                                                                         True\
                                                                    }}\
                                                                   }}\
                          }}}},\
                          {{"$addFields":{{\
                                          "GLT_incremental_number":{{"$cond":\
                                                                    {{"if":{{"$and":[{{"$eq":["$GLT_do_exception_exist",True]}},{{"$eq":["$GLT_exception_status","Closed"]}}]}}, \
                                                                     "then":\
                                                                         {{"$add":["$GLT_incremental_number",1]}}, \
                                                                     "else":\
                                                                         "$GLT_incremental_number" \
                                                                    }}\
                                                                   }}\
                          }}}},\
                           {{"$addFields":{{ \
                                         "GLT_do_discard":{{"$and":\
                                                           [{{"$eq":["$GLT_exception_status","Closed"]}},{{"$eq":["$IMDIF","X"]}}]}} \
                                         }} \
                            }},\
                         {{"$addFields":{{\
                          "GLT_history_exceptions_match" : {{"$concatArrays":[[{{"exceptionID": "$firstelem.exceptionID"\
                                                                             , "GLT_lastUpdatedDateTime": "$GLT_lastUpdatedDateTime"\
                                                                             , "COMPOSITEKEY": "$firstelem.COMPOSITEKEY"\
                                                                             , "GLT_incremental_number": "$firstelem.GLT_incremental_number"\
                                                                             , "exception_runID": "$firstelem.runID"\
                                                                             , "function_runID": "$runID"\
                                                                            }}]\
                                                                           ,{{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_exceptions_match",""]}}, ""]}}, "then":[], "else":"$GLT_history_exceptions_match"}}}}]\
                                              }}\
                        \
                         }}}},\
                          {{"$project": {{"firstelem":0, "matched_existing_exception":0}}}}\
						 ,{{"$merge" : {{ "into": "{function_id}"\
									  , "on": "_id" \
									  , "whenMatched":[ \
													 {{"$addFields":\
																{{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"\
																,"GLT_is_this_realized": "$$new.GLT_is_this_realized"\
                                                                ,"GLT_history_exceptions_match": "$$new.GLT_history_exceptions_match"\
                                                                ,"GLT_exception_status": "$$new.GLT_exception_status"\
                                                                ,"GLT_do_exception_exist": "$$new.GLT_do_exception_exist"\
                                                                ,"GLT_do_discard": "$$new.GLT_do_discard"\
                                                                ,"GLT_incremental_number": "$$new.GLT_incremental_number"\
																}}\
													 }}\
													]													\
									  , "whenNotMatched": "discard" }} \
						  }},\
						  ]'

AG_PIPELINE_FIN08_FA_1_4 = '[{{"$match": {{"runID": {{"$eq": "{run_id}"}},\
									 "GLT_is_this_realized": "IN-PROCESS"\
                                    ,"GLT_do_discard": False}}}}\
                            ,{{"$project": {{"_id": 0, "GLT_is_this_realized": 0\
                                       ,"GLT_do_exception_exist": 0, "GLT_do_discard":0\
                                       ,"GLT_history_exceptions_match":0, "GLT_exception_status":0}}}}\
						 ,{{"$addFields" : {{\
                                           "status": "Unassigned"\
										 , "control_id": "{control_id}"\
										 , "GLT_history_runID" : {{"$concatArrays":[[{{"runID": "$runID", "GLT_lastUpdatedDateTime": "$GLT_lastUpdatedDateTime"}}],{{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}}	\
										 , "exceptionID" : "" \
										 , "reason_code": ""  \
                                         , "GLT_do_auto_close": {{"$cond":{{"if":{{"$eq":["$IMDIF", "X"]}}, "then":True, "else":False}}}}\
                                         , "GLT_do_auto_reopen": False  \
										 }} \
						  }}\
						 ,{{"$merge" : {{ "into": "{exception_collection_name}"\
									  , "on": ["COMPOSITEKEY", "GLT_incremental_number"] \
									  , "whenMatched":[ \
													 {{"$addFields":\
																{{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"\
                                                                ,"runID": "$$new.runID"\
                                                                ,"FILENAME":"$$new.FILENAME"\
                                                                ,"GLT_do_auto_close":"$$new.GLT_do_auto_close"  \
                                                                ,"GLT_do_auto_reopen":False \
																,"GLT_history_runID": {{"$concatArrays":["$$new.GLT_history_runID", {{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}}  \
                                                                ,"exceptionID": {{"$toString": "$_id"}}\
                                                                ,"SLCOST":"$$new.SLCOST"\
                                                                ,"GLCOST":"$$new.GLCOST"\
                                                                ,"RECON":"$$new.RECON"\
                                                                ,"F08_ACUM_DEP_AMOUNT":"$$new.F08_ACUM_DEP_AMOUNT"\
                                                                ,"F08_UNPL_AMOUNT":"$$new.F08_UNPL_AMOUNT"\
                                                                ,"GL_ACUM_AMOUNT":"$$new.GL_ACUM_AMOUNT"\
                                                                ,"ASSET_ACUM_DEP":"$$new.ASSET_ACUM_DEP"\
                                                                ,"ACUM_RECON_DIFF":"$$new.ACUM_RECON_DIFF"\
                                                                ,"E_ACUM_RECON_DIFF":"$$new.E_ACUM_RECON_DIFF"\
                                                                ,"E_GL_RCON_DIFF":"$$new.E_GL_RCON_DIFF"\
                                                                ,"RATING":"$$new.RATING"\
                                                                ,"IMDIF":"$$new.IMDIF"\
                                                                }}\
													 }}\
													]													\
									  , "whenNotMatched": "insert"}} \
						  }}\
						  ]'

AG_PIPELINE_FIN08_AP_AR_1_3 = '[{{"$match": {{"runID": {{"$eq": "{run_id}"}},\
									 "GLT_is_this_realized":  {{"$ne": "DONE"}}}}\
                                                    }}\
                         ,{{"$limit": {limit_for_recs_processing_in_one_iteration}}}\
						 ,{{"$addFields" : {{"GLT_lastUpdatedDateTime": datetime.datetime.utcnow()\
										   , "GLT_is_this_realized": "IN-PROCESS"\
										  }}\
						  }}\
						 ,{{"$lookup": \
                           {{\
                               "from":"{exception_collection_name}"\
                              , "let": {{"foreignField":"$COMPOSITEKEY"}}\
                              ,"pipeline": [\
                                              {{"$match": \
                                               {{"$expr":\
                                                   {{ "$eq": [ "$COMPOSITEKEY",  "$$foreignField" ] }}\
                                               }}\
                                              }}\
                                             ,{{"$sort" : {{ "GLT_incremental_number" : -1}}}}\
                                           ]\
                              ,"as": "matched_existing_exception"\
                           }}\
                          }},\
                          {{"$addFields":{{"firstelem":{{"$first":"$matched_existing_exception"}}}}\
                            }},\
                          {{"$addFields":{{"GLT_exception_status":"$firstelem.status",\
                                         "GLT_incremental_number":{{"$ifNull":["$firstelem.GLT_incremental_number",1]}}\
                                        }}\
                          }},\
                          {{"$addFields":{{\
                                          "GLT_do_exception_exist":{{"$cond":\
                                                                    {{"if":{{\
                                                                        "$eq":[{{"$ifNull":["$GLT_exception_status","X"]}}, "X"]\
                                                                          }}, \
                                                                     "then":\
                                                                         False,\
                                                                     "else":\
                                                                         True\
                                                                    }}\
                                                                   }}\
                          }}}},\
                          {{"$addFields":{{\
                                          "GLT_incremental_number":{{"$cond":\
                                                                    {{"if":{{"$and":[{{"$eq":["$GLT_do_exception_exist",True]}},{{"$eq":["$GLT_exception_status","Closed"]}}]}}, \
                                                                     "then":\
                                                                         {{"$add":["$GLT_incremental_number",1]}},\
                                                                     "else":\
                                                                         "$GLT_incremental_number"\
                                                                    }}\
                                                                   }}\
                          }}}},\
                          {{"$addFields":{{\
                                         "GLT_do_discard":{{"$and":\
                                                           [{{"$eq":["$IMDIF","X"]}},\
                                                            {{"$or":[{{"$eq":["$GLT_exception_status","Closed"]}},{{"$eq":["$GLT_do_exception_exist",False]}}]}}\
                                                           ]\
                                                          }}\
                                         }}\
                            }},\
                        {{"$addFields":{{\
                        "GLT_history_exceptions_match" : {{"$concatArrays":[[{{"exceptionID": "$firstelem.exceptionID"\
                                                                             , "GLT_lastUpdatedDateTime": "$GLT_lastUpdatedDateTime"\
                                                                             , "COMPOSITEKEY": "$firstelem.COMPOSITEKEY"\
                                                                             , "exception_runID": "$firstelem.runID"\
                                                                             , "function_runID": "$runID"\
                                                                            }}]\
                                                                           ,{{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_exceptions_match",""]}}, ""]}}, "then":[], "else":"$GLT_history_exceptions_match"}}}}]\
                                              }}\
                        }}  }},\
                        {{"$project": {{"firstelem":0, "matched_existing_exception":0}}}}\
                        ,{{"$merge" : {{ "into": "{function_id}"\
									  , "on": "_id"\
									  , "whenMatched":[\
													 {{"$addFields":\
																{{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"\
																,"GLT_is_this_realized": "$$new.GLT_is_this_realized"\
                                                                ,"GLT_history_exceptions_match": "$$new.GLT_history_exceptions_match"\
                                                                ,"GLT_exception_status": "$$new.GLT_exception_status"\
                                                                ,"GLT_do_exception_exist": "$$new.GLT_do_exception_exist"\
                                                                ,"GLT_do_discard": "$$new.GLT_do_discard"\
                                                                ,"GLT_incremental_number": "$$new.GLT_incremental_number"\
																}}\
													 }}\
													]\
									  , "whenNotMatched": "discard" }}\
						  }},\
						  ]'

AG_PIPELINE_FIN08_AP_AR_1_4 = '[{{"$match": {{"runID": {{"$eq": "{run_id}"}},\
									 "GLT_is_this_realized": "IN-PROCESS"\
                                    ,"GLT_do_discard": False}}}}\
						 ,{{"$project": {{"_id": 0, "GLT_is_this_realized": 0\
                                       ,"GLT_do_exception_exist": 0, "GLT_do_discard":0\
                                       ,"GLT_history_exceptions_match":0, "GLT_exception_status":0}}}}\
						 ,{{"$addFields" : {{\
                                           "status": "Unassigned"\
										 , "control_id": "{control_id}"\
										 , "GLT_history_runID" : {{"$concatArrays":[[{{"runID": "$runID", "GLT_lastUpdatedDateTime": "$GLT_lastUpdatedDateTime"}}],{{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}}\
										 , "exceptionID" : ""\
										 , "reason_code": ""\
                                         , "GLT_do_auto_close": {{"$cond":{{"if":{{"$eq":["$IMDIF", "X"]}}, "then":True, "else":False}}}}\
                                         , "GLT_do_auto_reopen": False\
										 }}\
						  }}\
						 ,{{"$merge" : {{ "into": "{exception_collection_name}"\
									  , "on": ["COMPOSITEKEY", "GLT_incremental_number"]\
									  , "whenMatched":[\
													 {{"$addFields":\
																{{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"\
                                                                ,"runID": "$$new.runID"\
                                                                ,"FILENAME":"$$new.FILENAME"\
                                                                ,"GLT_do_auto_close":"$$new.GLT_do_auto_close"\
                                                                ,"GLT_do_auto_reopen":False\
																,"GLT_history_runID": {{"$concatArrays":["$$new.GLT_history_runID", {{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}}\
                                                                ,"exceptionID": {{"$toString": "$_id"}}\
                                                                ,"BPTYPE":"$$new.BPTYPE"\
                                                                ,"BPTYP1":"$$new.BPTYP1"\
                                                                ,"IMDIF":"$$new.IMDIF"\
                                                                ,"DMBTR4":"$$new.DMBTR4"\
                                                                ,"DMBTR1":"$$new.DMBTR1"\
                                                                ,"FS10N":"$$new.FS10N"\
                                                                ,"DMBTR3":"$$new.DMBTR3"\
                                                                ,"BPCLASS":"$$new.BPCLASS"\
                                                                ,"RATING":"$$new.RATING"\
                                                                }}\
													 }}\
													]\
									  , "whenNotMatched": "insert" }}\
						  }}\
						  ]'

AG_PIPELINE_FIN08_INVENTORY_1_3 = '[{{"$match": {{"runID": {{"$eq": "{run_id}" }}\
                                                                 ,"GLT_is_this_realized": {{"$ne": "DONE"}} \
                                                                }}\
                                                     }}, \
						  {{"$limit": {limit_for_recs_processing_in_one_iteration}}}\
						 ,{{"$addFields" : {{"GLT_lastUpdatedDateTime": datetime.datetime.utcnow()\
										 , "GLT_is_this_realized": "IN-PROCESS"	 \
										  }}\
						  }}\
                         ,{{"$lookup": \
                           {{\
                               "from":"{exception_collection_name}"\
                              , "let": {{"foreignField":"$COMPOSITEKEY"}}\
                              ,"pipeline": [\
                                              {{"$match": \
                                               {{"$expr":\
                                                   {{ "$eq": [ "$COMPOSITEKEY",  "$$foreignField" ] }}\
                                               }}\
                                              }}\
                                             ,{{"$sort" : {{"GLT_incremental_number": -1}}}}                         \
                                           ]\
                              ,"as": "matched_existing_exception"\
                           }}\
                          }},\
                          {{"$addFields":{{"firstelem":{{"$first":"$matched_existing_exception"}}}}\
                            }},\
                          {{"$addFields":{{"GLT_exception_status":"$firstelem.status",\
                                         "GLT_incremental_number":{{"$ifNull":["$firstelem.GLT_incremental_number",1]}} \
                                        }}\
                          }},\
                          {{"$addFields":{{\
                                          "GLT_do_exception_exist":{{"$cond":\
                                                                    {{"if":{{\
                                                                        "$eq":[{{"$ifNull":["$GLT_exception_status","X"]}}, "X"]\
                                                                          }}, \
                                                                     "then":\
                                                                         False,\
                                                                     "else":\
                                                                         True\
                                                                    }}\
                                                                   }}\
                          }}}},\
                          {{"$addFields":{{\
                                          "GLT_incremental_number":{{"$cond":\
                                                                    {{"if":{{"$and":[{{"$eq":["$GLT_do_exception_exist",True]}},{{"$eq":["$GLT_exception_status","Closed"]}}]}}, \
                                                                     "then":\
                                                                         {{"$add":["$GLT_incremental_number",1]}}, \
                                                                     "else":\
                                                                         "$GLT_incremental_number" \
                                                                    }}\
                                                                   }}\
                          }}}},\
                           {{"$addFields":{{ \
                                         "GLT_do_discard":{{"$and":\
                                                           [{{"$eq":["$IMDIF","X"]}},\
                                                            {{"$or":[{{"$eq":["$GLT_exception_status","Closed"]}},{{"$eq":["$GLT_do_exception_exist",False]}}]}}\
                                                           ]\
                                                          }}                                             \
                                         }}                              \
                            }},\
                         {{"$addFields":{{\
                          "GLT_history_exceptions_match" : {{"$concatArrays":[[{{"exceptionID": "$firstelem.exceptionID"\
                                                                             , "GLT_lastUpdatedDateTime": "$GLT_lastUpdatedDateTime"\
                                                                             , "COMPOSITEKEY": "$firstelem.COMPOSITEKEY"\
                                                                             , "GLT_incremental_number": "$firstelem.GLT_incremental_number"\
                                                                             , "exception_runID": "$firstelem.runID"\
                                                                             , "function_runID": "$runID"\
                                                                            }}]\
                                                                           ,{{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_exceptions_match",""]}}, ""]}}, "then":[], "else":"$GLT_history_exceptions_match"}}}}]\
                                              }}\
                        \
                         }}}},\
                          {{"$project": {{"firstelem":0, "matched_existing_exception":0}}}}\
						 ,{{"$merge" : {{ "into": "{function_id}"\
									  , "on": "_id" \
									  , "whenMatched":[ \
													 {{"$addFields":\
																{{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"\
																,"GLT_is_this_realized": "$$new.GLT_is_this_realized"\
                                                                ,"GLT_history_exceptions_match": "$$new.GLT_history_exceptions_match"\
                                                                ,"GLT_exception_status": "$$new.GLT_exception_status"\
                                                                ,"GLT_do_exception_exist": "$$new.GLT_do_exception_exist"\
                                                                ,"GLT_do_discard": "$$new.GLT_do_discard"\
                                                                ,"GLT_incremental_number": "$$new.GLT_incremental_number"\
																}}\
													 }}\
													]													\
									  , "whenNotMatched": "discard" }} \
						  }},\
						  ]'

AG_PIPELINE_FIN08_INVENTORY_1_4 = '[{{"$match": {{"runID": {{"$eq": "{run_id}"}},\
									 "GLT_is_this_realized": "IN-PROCESS"\
                                    ,"GLT_do_discard": False}}}}\
						 ,{{"$project": {{"_id": 0, "GLT_is_this_realized": 0\
                                       ,"GLT_do_exception_exist": 0, "GLT_do_discard":0\
                                       ,"GLT_history_exceptions_match":0, "GLT_exception_status":0}}}}	\
						 ,{{"$addFields" : {{\
                                           "status": "Unassigned"	\
										 , "control_id": "{control_id}"\
										 , "GLT_history_runID" : {{"$concatArrays":[[{{"runID": "$runID", "GLT_lastUpdatedDateTime": "$GLT_lastUpdatedDateTime"}}],{{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}}	\
										 , "exceptionID" : "" \
										 , "reason_code": ""  \
                                         , "GLT_do_auto_close": {{"$cond":{{"if":{{"$eq":["$IMDIF", "X"]}}, "then":True, "else":False}}}}\
                                         , "GLT_do_auto_reopen": False  \
										 }} \
						  }}\
						 ,{{"$merge" : {{ "into": "{exception_collection_name}"\
									  , "on": ["COMPOSITEKEY", "GLT_incremental_number"] \
									  , "whenMatched":[ \
													 {{"$addFields":\
																{{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"\
                                                                ,"runID": "$$new.runID"\
                                                                ,"FILENAME":"$$new.FILENAME"\
                                                                ,"GLT_do_auto_close":"$$new.GLT_do_auto_close"  \
                                                                ,"GLT_do_auto_reopen":False \
																,"GLT_history_runID": {{"$concatArrays":["$$new.GLT_history_runID", {{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}} \
                                                                ,"exceptionID": {{"$toString": "$_id"}}  \
                                                                ,"SALK3":"$$new.SALK3"\
                                                                ,"IMDIF":"$$new.IMDIF"\
                                                                ,"DMBTR1":"$$new.DMBTR1"\
                                                                ,"DMBTR2":"$$new.DMBTR2"\
                                                                ,"DMBTR3":"$$new.DMBTR3"\
                                                                ,"DMBTR4":"$$new.DMBTR4"\
                                                                ,"LBKUM":"$$new.LBKUM"\
                                                                ,"MEINS":"$$new.MEINS"\
                                                                ,"MEC1":"$$new.MEC1"\
                                                                ,"RATING":"$$new.RATING"\
                                                                }}\
													 }}\
													]	\
									  , "whenNotMatched": "insert"}} \
						  }}\
						  ]'