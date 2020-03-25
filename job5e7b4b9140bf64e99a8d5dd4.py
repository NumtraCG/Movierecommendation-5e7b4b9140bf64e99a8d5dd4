import traceback
import sys
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e7b4b9140bf64e99a8d5dd5','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify')
	Movie_recommendation_DBFS = DBFSConnector.DBFSConnector.fetch([], {}, "5e7b4b9140bf64e99a8d5dd5", spark, "{'url': '/Demo/MovieRatingsTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapib8073bbfa952efa9d363b234ce06e2c6', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e7b4b9140bf64e99a8d5dd5','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e7b4b9140bf64e99a8d5dd5','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify','http://40.83.140.93:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e7b4b9140bf64e99a8d5dd6','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify')
	Movie_recommendation_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e7b4b9140bf64e99a8d5dd5"],{"5e7b4b9140bf64e99a8d5dd5": Movie_recommendation_DBFS}, "5e7b4b9140bf64e99a8d5dd6", spark,json.dumps( {"FE": [{"transformationsData": {}, "feature": "UserId", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2587", "mean": "465.06", "stddev": "264.69", "min": "1", "max": "943", "missing": "0"}}, {"transformationsData": {}, "feature": "MovieId", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2587", "mean": "432.85", "stddev": "337.75", "min": "1", "max": "1656", "missing": "0"}}, {"transformationsData": {}, "feature": "Rating", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "2587", "mean": "3.52", "stddev": "1.15", "min": "1.0", "max": "5.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {"feature_label": "Timestamp"}, "feature": "Timestamp", "type": "date", "selected": "True", "replaceby": "random", "stats": {"count": "", "mean": "", "stddev": "", "min": "", "max": "", "missing": "0"}, "transformation": "Extract Date"}, {"transformationsData": {}, "feature": "AvgRating", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "2587", "mean": "3.53", "stddev": "0.44", "min": "1.51", "max": "4.67", "missing": "0"}, "transformation": ""}, {"feature": "Timestamp_dayofmonth", "transformation": "", "transformationsData": {}, "type": "numeric", "generated": "True", "selected": "True", "stats": {"count": "2587", "mean": "16.05", "stddev": "9.06", "min": "1", "max": "31", "missing": "0"}}, {"feature": "Timestamp_month", "transformation": "", "transformationsData": {}, "type": "numeric", "generated": "True", "selected": "True", "stats": {"count": "2587", "mean": "7.0", "stddev": "4.34", "min": "1", "max": "12", "missing": "0"}}, {"feature": "Timestamp_year", "transformation": "", "transformationsData": {}, "type": "numeric", "generated": "True", "selected": "True", "stats": {"count": "2587", "mean": "1997.45", "stddev": "0.5", "min": "1997", "max": "1998", "missing": "0"}}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e7b4b9140bf64e99a8d5dd6','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e7b4b9140bf64e99a8d5dd6','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify','http://40.83.140.93:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e7b4b9140bf64e99a8d5dd7','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify')
	Movie_recommendation_AutoML = tpot_execution.Tpot_execution.run(["5e7b4b9140bf64e99a8d5dd6"],{"5e7b4b9140bf64e99a8d5dd6": Movie_recommendation_AutoFE}, "5e7b4b9140bf64e99a8d5dd7", spark,json.dumps( {"model_type": "classification", "label": "Rating", "features": ["UserId", "MovieId", "Timestamp", "AvgRating"], "percentage": "10", "executionTime": "5", "sampling": "0", "sampling_value": "none", "run_id": "", "ProjectName": "zeshan_test", "PipelineName": "Movierecommendation", "pipelineId": "5e7b4b9140bf64e99a8d5dd4", "userid": "5e39734e0204cd465d4d2e10", "runid": "", "url_ResultView": "http://40.83.140.93:3200", "experiment_id": "551308251382540"}))

	PipelineNotification.PipelineNotification().completed_notification('5e7b4b9140bf64e99a8d5dd7','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e7b4b9140bf64e99a8d5dd7','5e39734e0204cd465d4d2e10','http://40.83.140.93:3200/pipeline/notify','http://40.83.140.93:3200/logs/getProductLogs')
	sys.exit(1)

