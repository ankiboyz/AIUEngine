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
											  ,"GLT_is_this_realized": {{"$ne": "DONE"}}\
											  }}\
								  }}, \
								  {{"$limit": {limit_for_recs_processing_in_one_iteration} }}\
								 ,{{"$addFields" : {{"GLT_lastUpdatedDateTime": datetime.datetime.utcnow()\
												 , "GLT_is_this_realized": "IN-PROCESS"\
												 ,"GLT_whether_S2A": {{"$cond": {{"if": {{"$and": [{{"$eq":["$EXCEPTION", "S2"]}},{{"$or":[{{"$eq":["$KOSTL_VALID", "TRUE"]}},{{"$eq":["$CAUFN_VALID", "TRUE"]}},{{"$eq":["$POSNR_VALID", "TRUE"]}}]}}]}}, "then": True,"else": False}}\
												  }}}}\
								  }}\
								 ,{{"$merge" : {{ "into": "{function_id}"\
											  , "on": "_id" \
											  , "whenMatched":[ \
															 {{"$addFields":\
																		{{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"\
																		,"GLT_is_this_realized": "$$new.GLT_is_this_realized"\
																		,"GLT_whether_S2A":"$$new.GLT_whether_S2A"\
																		}}\
															 }}\
															]\
											  , "whenNotMatched": "discard" }}\
								  }},\
								  ]'

# This is the Auto-Close condition based on Auto Close key matches. This is put before the newer docs of the same file
# being inserted so that the comparision is not made with the recs of the same file.
AG_PIPELINE_TFA02_IFA19_1_4 = '[{{"$match": {{"runID": {{"$eq": "{run_id}" }}\
										,"GLT_is_this_realized": "IN-PROCESS"\
										,"GLT_whether_S2A": True\
										}}}}\
							 ,{{"$addFields" : {{\
											   "GLT_whether_S2A": True\
											 , "status": "Unassigned"\
											 , "control_id": "{control_id}"\
											 , "GLT_history_runID" : [{{"GLT_function_doc_id": "$_id","runID": "$runID", "GLT_lastUpdatedDateTime": "$GLT_lastUpdatedDateTime"}}]	\
											 , "exceptionID" : ""\
											 , "reason_code": ""\
											 }}\
							  \
							  }} \
							 ,{{"$project": {{"_id": 0, "GLT_is_this_realized": 0}}}} \
							 ,{{"$lookup":\
							   {{\
								   "from": "{exception_collection_name}"\
								 , "let": {{ "curr_rec_BUKRS":"$BUKRS"\
										 , "curr_rec_ANLN1":"$ANLN1", "curr_rec_ANLN2":"$ANLN2"\
										 , "curr_rec_FNAME":"$FNAME", "curr_rec_EXCEPTION":"$EXCEPTION"\
										 , "curr_rec_SYSTEM":"$SYSTEM" \
										  }}\
								 , "pipeline": [\
												  {{"$match": \
												   {{"$and":[ \
													   {{"$expr":\
														   {{ "$eq": [ "$BUKRS",  "$$curr_rec_BUKRS" ] }}\
													   }},\
													   {{"$expr":\
														   {{ "$eq": [ "$ANLN1",  "$$curr_rec_ANLN1" ] }}\
													   }},\
													   {{"$expr":\
														   {{ "$eq": [ "$ANLN2",  "$$curr_rec_ANLN2" ] }}\
													   }},                                                   \
													   {{"$expr":\
														   {{ "$eq": [ "$FNAME",  "$$curr_rec_FNAME" ] }}\
													   }},\
													   {{"$expr":\
														   {{ "$eq": [ "$EXCEPTION",  "$$curr_rec_EXCEPTION" ] }}\
													   }},\
													   {{"$expr":\
														   {{ "$eq": [ "$SYSTEM",  "$$curr_rec_SYSTEM" ] }}\
													   }},  \
													   {{"$expr":\
														   {{ "$ne": [ "$status",  "Closed" ] }} \
													   }},  \
												   ]\
												  }}\
												  }}\
											   ]\
								  ,"as": "matched_existing_exception"\
							   }}\
							  }},\
							  {{"$unwind":\
								{{\
								 "path":"$matched_existing_exception"\
								}}  \
							  }},\
							  {{"$addFields":\
								  {{\
								 "COMPOSITEKEY":{{"$cond":\
														 {{"if":{{"$or":[{{"$eq":["$matched_existing_exception.KOSTL_VALID","FALSE"]}}\
																	   ,{{"$eq":["$matched_existing_exception.CAUFN_VALID","FALSE"]}}\
																	   ,{{"$eq":["$matched_existing_exception.POSNR_VALID","FALSE"]}}\
																		]\
																}}                                                            \
														  ,"then":\
														  "$matched_existing_exception.COMPOSITEKEY"\
														  ,"else":\
														   "X" \
														  }}\
												}}\
								,"GLT_do_auto_close": True \
								  }}  \
							  }},\
							  {{"$project":\
								  {{"COMPOSITEKEY": 1, "GLT_history_runID": 1\
								  ,"runID": 1, "FILENAME": 1, "control_id": 1\
								  ,"GLT_lastUpdatedDateTime": 1, "GLT_whether_S2A": 1\
								  ,"USNAM":1, "NAME_TEXT":1, "KOSTL":1, "KOSTL_VALID":1, "CAUFN":1, "CAUFN_VALID":1\
								  ,"WBS":1, "POSNR_VALID":1, "UDATE":1, "UTIME":1\
								  ,"TCODE":1, "CNGID":1, "VALUE_NEW":1, "VALUE_OLD":1\
								  ,"AVFACC":1, "AVEUR":1, "RATING":1, "EXCEPTION":1, "CDHDR_OBJCLAS":1 \
								  ,"CDHDR_OBJCTID":1, "CDHDR_CHANGENR":1, "CDPOS_OBJCLAS":1, "CDPOS_OBJTID":1\
								  ,"CDPOS_CHANGENR":1, "GLT_do_auto_close":1\
								  }}\
							  }},\
							  {{"$merge" : {{ "into": "{exception_collection_name}"\
										  , "on": "COMPOSITEKEY"\
										  , "whenMatched":[ \
														 {{"$addFields":\
																	{{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"\
																	,"GLT_do_auto_close":"$$new.GLT_do_auto_close"\
																	,"GLT_whether_S2A": "$$new.GLT_whether_S2A"\
																	,"GLT_history_runID": {{"$concatArrays":["$$new.GLT_history_runID", {{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}} \
																	,"exceptionID": {{"$toString": "$_id"}}\
																	,"USNAM":"$$new.USNAM"\
																	,"NAME_TEXT":"$$new.NAME_TEXT"\
																	,"KOSTL":"$$new.KOSTL"\
																	,"KOSTL_VALID":"$$new.KOSTL_VALID"\
																	,"CAUFN":"$$new.CAUFN"\
																	,"CAUFN_VALID":"$$new.CAUFN_VALID"\
																	,"WBS":"$$new.WBS"\
																	,"POSNR_VALID":"$$new.POSNR_VALID"\
																	,"UDATE":"$$new.UDATE"\
																	,"UTIME":"$$new.UTIME"\
																	,"TCODE":"$$new.TCODE"\
																	,"CNGID":"$$new.CNGID"\
																	,"VALUE_NEW":"$$new.VALUE_NEW"\
																	,"VALUE_OLD":"$$new.VALUE_OLD"\
																	,"AVFACC":"$$new.AVFACC"\
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
														]													\
										  , "whenNotMatched": "discard" }}\
							  }}\
							  ]'

# This is the 4.5th Stage ; for the newer recs that are NOT S2A to be inserted into the exception collection.
AG_PIPELINE_TFA02_IFA19_1_4_5 = '[{{"$match": {{"runID": {{"$eq": "{run_id}"}}\
											,"GLT_is_this_realized": "IN-PROCESS"\
											,"GLT_whether_S2A": False  \
											}}}}\
								  ,{{"$addFields" : {{\
												   "GLT_whether_S2A": False	\
												 , "status": "Unassigned"	\
												 , "control_id": "{control_id}"\
												 , "GLT_history_runID" : [{{"GLT_function_doc_id": "$_id","runID": "$runID", "GLT_lastUpdatedDateTime": "$GLT_lastUpdatedDateTime"}}]	\
												 , "exceptionID" : "" \
												 , "reason_code": ""  \
												 }}\
								  \
								  }}\
								 ,{{"$project": {{"_id": 0, "GLT_is_this_realized": 0}}}}	\
								 ,{{"$addFields" : {{\
												"GLT_do_auto_close": {{"$cond": {{"if": {{"$eq":["$RATING", "LOW"]}}, "then": True,"else": False}}}}\
												}}\
								  }}\
								 ,{{"$merge" : {{ "into": "{exception_collection_name}"\
											  , "on": "COMPOSITEKEY"\
											  , "whenMatched":[ \
															 {{"$addFields":\
																		{{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"\
																		,"GLT_do_auto_close":"$$new.GLT_do_auto_close"\
																		,"GLT_whether_S2A": "$$new.GLT_whether_S2A"\
																		,"GLT_history_runID": {{"$concatArrays":["$$new.GLT_history_runID", {{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}} \
																		,"exceptionID": {{"$toString": "$_id"}}\
																		,"USNAM":"$$new.USNAM"\
																		,"NAME_TEXT":"$$new.NAME_TEXT"\
																		,"KOSTL":"$$new.KOSTL"\
																		,"KOSTL_VALID":"$$new.KOSTL_VALID"\
																		,"CAUFN":"$$new.CAUFN"\
																		,"CAUFN_VALID":"$$new.CAUFN_VALID"\
																		,"WBS":"$$new.WBS"\
																		,"POSNR_VALID":"$$new.POSNR_VALID"\
																		,"UDATE":"$$new.UDATE"\
																		,"UTIME":"$$new.UTIME"\
																		,"TCODE":"$$new.TCODE"\
																		,"CNGID":"$$new.CNGID"\
																		,"VALUE_NEW":"$$new.VALUE_NEW"\
																		,"VALUE_OLD":"$$new.VALUE_OLD"\
																		,"AVFACC":"$$new.AVFACC"\
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
																,"EXP_STATUS":"$$new.EXP_STATUS"\
                                                                ,"AUGDT":"$$new.AUGDT"\
                                                                ,"AUGBL":"$$new.AUGBL"\
																,"XSTOV":"$$new.XSTOV"\
																,"XREVERSAL":"$$new.XREVERSAL"\
																,"XRAGL":"$$new.XRAGL"\
																}}\
                                                     }}\
                                                    ]\
                                      , "whenNotMatched": "insert" }}\
                          }}\
                          ]'

# This is 4.5th stage; added for Auto Reopen scenarios for TRE07.
AG_PIPELINE_TRE07_1_4_5 = '[{{"$match": {{"runID": {{"$eq": "{run_id}"}},\
									 "GLT_is_this_fresh": True, "XREVERSAL": "2", "EXP_STATUS":"Cleared Item"}}}}\
						 ,{{"$lookup": \
                           {{\
                               "from": "{exception_collection_name}"\
                             , "let": {{ "curr_rec_BELNR":"$BELNR"\
                                     , "curr_rec_BUKRS":"$BUKRS", "curr_rec_GJAHR":"$GJAHR"\
                                     , "curr_rec_HKONT":"$HKONT", "curr_rec_SAP":"$SAP"\
                                     , "curr_rec_EXP_STATUS":"$EXP_STATUS" \
                                      }}\
                             , "pipeline": [\
                                              {{"$match": \
                                               {{"$and":[ \
                                                   {{"$expr":\
                                                       {{ "$eq": [ "$AUGBL",  "$$curr_rec_BELNR" ] }} \
                                                   }},\
                                                   {{"$expr":\
                                                       {{ "$eq": [ "$GJAHR",  "$$curr_rec_GJAHR" ] }}\
                                                   }},\
                                                   {{"$expr":\
                                                       {{ "$eq": [ "$BUKRS",  "$$curr_rec_BUKRS" ] }}\
                                                   }},\
                                                   {{"$expr":\
                                                       {{ "$eq": [ "$HKONT",  "$$curr_rec_HKONT" ] }}\
                                                   }},\
                                                   {{"$expr":\
                                                       {{ "$eq": [ "$SAP",  "$$curr_rec_SAP" ] }}\
                                                   }},\
                                                   {{"$expr":\
                                                       {{ "$eq": [ "$EXP_STATUS",  "$$curr_rec_EXP_STATUS" ] }}\
                                                   }},\
                                               ]\
                                              }}\
                                              }}\
                                           ]\
                              ,"as": "matched_existing_exception"\
                           }}\
                          }},\
                          {{"$unwind":\
                            {{\
                             "path":"$matched_existing_exception"\
                            }}  \
                          }},\
                          {{"$addFields":\
                              {{\
                               "COMPOSITEKEY": "$matched_existing_exception.COMPOSITEKEY"\
                             , "GLT_history_runID" : [{{"runID": "$runID", "GLT_lastUpdatedDateTime": "$GLT_lastUpdatedDateTime"}}]\
                              }}\
                          }},\
                          {{"$project":\
                              {{"COMPOSITEKEY": 1, "GLT_history_runID": 1\
                              ,"runID": 1, "FILENAME": 1\
                              ,"GLT_lastUpdatedDateTime": 1, "XSTOV": 1\
                              ,"XREVERSAL": 1, "XRAGL": 1\
                              }}\
                          }}\
                        ,{{"$merge" : {{ "into": "{exception_collection_name}"\
									  , "on": "COMPOSITEKEY"\
									  , "whenMatched":[\
													 {{"$addFields":\
																{{"GLT_lastUpdatedDateTime": "$$new.GLT_lastUpdatedDateTime"\
                                                                ,"runID": "$$new.runID" \
                                                                ,"FILENAME":"$$new.FILENAME"\
                                                                ,"GLT_do_auto_close":False\
                                                                ,"GLT_do_auto_reopen":True\
																,"GLT_history_runID": {{"$concatArrays":["$$new.GLT_history_runID", {{"$cond":{{"if":{{"$eq":[{{"$ifNull":["$GLT_history_runID",""]}}, ""]}}, "then":[], "else":"$GLT_history_runID"}}}}]}}\
																 , "GLT_is_this_fresh": False\
																 ,"EXP_STATUS":"Open Item"\
                                                                 ,"AUGDT":""\
                                                                 ,"AUGBL":""\
																 ,"XSTOV":"$$new.XSTOV"\
																 ,"XREVERSAL":"$$new.XREVERSAL"\
																 ,"XRAGL":"$$new.XRAGL"\
                                                                }}\
													 }}\
													]\
									  , "whenNotMatched": "discard" }}\
						  }}                      \
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